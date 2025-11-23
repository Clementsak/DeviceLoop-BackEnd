import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key

# --- Setup ---

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")  # provided by Lambda automatically
DDB_TABLE_NAME = os.environ["DDB_TABLE_NAME"]

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DDB_TABLE_NAME)

sns = boto3.client("sns")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")


def lambda_handler(event, context):
    """
    Entry point for Amazon Simple Queue Service triggered Lambda.
    For each Amazon Simple Queue Service record, we expect a JSON body like:

      {
        "type": "NEW_BID",
        "marketKey": "...",
        "bidPk": "MARKET#...",
        "bidSk": "BID#...",
        "auctionMode": "continuous" | "interval" | "end_of_window"
      }

    or

      {
        "type": "NEW_LISTING",
        "marketKey": "...",
        "listingId": "LISTREQ#SELLER#..."
      }
    """
    print("Received event:", json.dumps(event))

    for record in event.get("Records", []):
        body = record.get("body")
        if not body:
            continue

        try:
            msg = json.loads(body)
        except json.JSONDecodeError:
            print("Skipping non JSON message:", body)
            continue

        msg_type = msg.get("type")
        if msg_type == "NEW_BID":
            handle_new_bid_message(msg)
        elif msg_type == "NEW_LISTING":
            handle_new_listing_message(msg)
        else:
            print("Unknown message type:", msg_type)

    return {"ok": True}


def handle_new_bid_message(msg: dict) -> None:
    """
    Handle a NEW_BID message from the backend.

    We fully implement the continuous mode:
    - Load the bid
    - Check status and expiry
    - If auctionMode == 'continuous', try to match it immediately
    """
    market_key = msg.get("marketKey")
    bid_pk = msg.get("bidPk")
    bid_sk = msg.get("bidSk")
    auction_mode = msg.get("auctionMode") or "continuous"

    print(f"[NEW_BID] market={market_key}, pk={bid_pk}, sk={bid_sk}, mode={auction_mode}")

    if not bid_pk or not bid_sk:
        print("Missing bidPk or bidSk in message, skipping")
        return

    # 1) Load the bid item from DynamoDB
    bid_resp = table.get_item(Key={"PK": bid_pk, "SK": bid_sk})
    bid = bid_resp.get("Item")
    if not bid:
        print("Bid item not found in DynamoDB, maybe already deleted")
        return

    now = datetime.now(timezone.utc)

    # Status and expiry check
    status = bid.get("BidStatus", "open")
    if status != "open":
        print(f"Bid is not open (status={status}), skipping")
        return

    expires_at_str = bid.get("BidExpiresAt")
    if expires_at_str:
        try:
            expires_at = datetime.fromisoformat(expires_at_str)
            if now >= expires_at:
                print("Bid already expired, marking as expired")
                table.update_item(
                    Key={"PK": bid_pk, "SK": bid_sk},
                    UpdateExpression="SET BidStatus = :s",
                    ExpressionAttributeValues={":s": "expired"},
                )
                return
        except ValueError:
            print("Could not parse BidExpiresAt:", expires_at_str)

    # Only continuous mode does immediate matching here.
    if auction_mode != "continuous":
        print(f"Auction mode is {auction_mode}, this Lambda only does continuous matching.")
        return

    try:
        bid_price = float(bid["BidPrice"])
    except (KeyError, TypeError, ValueError):
        print("Bid has invalid BidPrice, skipping:", bid.get("BidPrice"))
        return

    buyer_pk = bid.get("BuyerPK")
    print(f"Attempting continuous match for buyer={buyer_pk}, price={bid_price}")

    # 2) Load active sellers for this marketKey.
    # Note: we use a table scan with a filter expression for simplicity.
    scan_kwargs = {
        "FilterExpression": (
            Attr("SK").eq("LISTING_REQUEST")
            & Attr("MarketKey").eq(market_key)
            & Attr("Status").eq("active")
            & Attr("AuctionMode").eq("continuous")
        )
    }

    sellers = []
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key
        resp = table.scan(**scan_kwargs)
        sellers.extend(resp.get("Items", []))
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print(f"Found {len(sellers)} active continuous listings for market {market_key}")

    # Filter by auction window and price envelope; then sort by SellerMin
    candidates = []
    for s in sellers:
        if not is_within_auction_window(s, now):
            continue

        final_min = s.get("FinalMin") or s.get("InitialMin")
        final_max = s.get("FinalMax") or s.get("InitialMax")

        final_min = float(final_min) if isinstance(final_min, (int, float, Decimal)) else None
        final_max = float(final_max) if isinstance(final_max, (int, float, Decimal)) else None

        seller_min = s.get("SellerMin")
        seller_max = s.get("SellerMax")
        seller_min = float(seller_min) if isinstance(seller_min, (int, float, Decimal)) else None
        seller_max = float(seller_max) if isinstance(seller_max, (int, float, Decimal)) else None

        if seller_min is None:
            continue

        # Envelope checks
        if final_min is not None and bid_price < final_min:
            continue
        if final_max is not None and bid_price > final_max:
            continue
        if seller_max is not None and bid_price > seller_max:
            continue

        candidates.append(
            {
                "raw": s,
                "seller_min": seller_min,
                "final_min": final_min,
                "final_max": final_max,
            }
        )

    if not candidates:
        print("No suitable active listings found for this bid.")
        return

    candidates.sort(key=lambda c: c["seller_min"])

    # 3) Try to match against the best seller (first that satisfies bid >= seller_min)
    for c in candidates:
        s = c["raw"]
        seller_min = c["seller_min"]

        if bid_price < seller_min:
            continue

        trade_price = (bid_price + seller_min) / 2.0
        print(
            f"Match found! buyer={buyer_pk}, listing={s['PK']}, "
            f"seller_min={seller_min}, bid={bid_price}, trade={trade_price}"
        )

        write_trade_item(
            listing=s,
            buyer_pk=buyer_pk,
            bid_price=bid_price,
            ask_price=seller_min,
            trade_price=trade_price,
            now=now,
        )

        write_trade_notifications(
            listing_item=s,
            buyer_pk=buyer_pk,
            trade_price=Decimal(str(trade_price)),
            now_iso=now.isoformat(),
        )

        # Update listing as ended or sold
        table.update_item(
            Key={"PK": s["PK"], "SK": s["SK"]},
            UpdateExpression=(
                "SET #st = :ended, "
                "MatchedBuyerPK = :b, "
                "MatchedTradePrice = :tp, "
                "MatchedAt = :now, "
                "CurrentHighestBid = :cbid, "
                "CurrentHighestBidderPK = :b "
            ),
            ExpressionAttributeNames={"#st": "Status"},
            ExpressionAttributeValues={
                ":ended": "ended",
                ":b": buyer_pk,
                ":tp": Decimal(str(trade_price)),
                ":now": now.isoformat(),
                ":cbid": Decimal(str(bid_price)),
            },
        )

        # Mark bid as filled
        table.update_item(
            Key={"PK": bid_pk, "SK": bid_sk},
            UpdateExpression="SET BidStatus = :s",
            ExpressionAttributeValues={":s": "filled"},
        )

        print("Match persisted to DynamoDB.")
        return

    print("No seller satisfied bid >= seller_min; bid remains open.")


def handle_new_listing_message(msg: dict) -> None:
    """
    Handle a NEW_LISTING message:
    - Load the new listing
    - If it is an active continuous auction inside its window,
      search for open bids in that market and match the best one.
    """
    market_key = msg.get("marketKey")
    listing_id = msg.get("listingId")

    print(f"[NEW_LISTING] market={market_key}, listingId={listing_id}")

    if not market_key or not listing_id:
        print("Missing marketKey or listingId in NEW_LISTING message, skipping")
        return

    # Load listing
    resp = table.get_item(Key={"PK": listing_id, "SK": "LISTING_REQUEST"})
    listing = resp.get("Item")
    if not listing:
        print("Listing not found in DynamoDB, maybe already deleted")
        return

    status = listing.get("Status")
    if status != "active":
        print(f"Listing is not active (status={status}), skipping")
        return

    auction_mode = listing.get("AuctionMode") or "continuous"
    if auction_mode != "continuous":
        print(f"Listing auction mode is {auction_mode}, only continuous mode is matched here")
        return

    now = datetime.now(timezone.utc)
    if not is_within_auction_window(listing, now):
        print("Listing is not inside its auction window, skipping")
        return

    # Seller and platform envelopes
    final_min = listing.get("FinalMin") or listing.get("InitialMin")
    final_max = listing.get("FinalMax") or listing.get("InitialMax")
    if isinstance(final_min, Decimal):
        final_min = float(final_min)
    if isinstance(final_max, Decimal):
        final_max = float(final_max)

    seller_min = listing.get("SellerMin")
    seller_max = listing.get("SellerMax")
    if isinstance(seller_min, Decimal):
        seller_min = float(seller_min)
    if isinstance(seller_max, Decimal):
        seller_max = float(seller_max)

    if seller_min is None:
        print("Listing has no SellerMin; cannot run continuous matching")
        return

    # Load open bids for this market
    pk = f"MARKET#{market_key}"
    query_kwargs = {
        "KeyConditionExpression": Key("PK").eq(pk),
        "FilterExpression": Attr("BidStatus").eq("open"),
    }

    bids = []
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            query_kwargs["ExclusiveStartKey"] = last_evaluated_key
        resp = table.query(**query_kwargs)
        for item in resp.get("Items", []):
            exp_str = item.get("BidExpiresAt")
            if exp_str:
                try:
                    exp = datetime.fromisoformat(exp_str)
                    if now >= exp:
                        continue
                except Exception:
                    continue
            bids.append(item)
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print(f"Found {len(bids)} open bids for market {market_key}")

    # Filter bids that fit the envelopes
    candidate_bids = []
    for b in bids:
        price = b.get("BidPrice")
        if isinstance(price, Decimal):
            price = float(price)
        if not isinstance(price, (int, float)):
            continue
        bid_price = float(price)

        if final_min is not None and bid_price < final_min:
            continue
        if final_max is not None and bid_price > final_max:
            continue
        if seller_min is not None and bid_price < seller_min:
            continue
        if seller_max is not None and bid_price > seller_max:
            continue

        candidate_bids.append({"raw": b, "price": bid_price})

    if not candidate_bids:
        print("No suitable open bids found for this new listing.")
        return

    # Choose highest bid
    candidate_bids.sort(key=lambda x: x["price"], reverse=True)
    best_bid = candidate_bids[0]
    bid_item = best_bid["raw"]
    bid_price = best_bid["price"]
    buyer_pk = bid_item.get("BuyerPK")

    trade_price = (bid_price + seller_min) / 2.0
    print(
        f"[NEW_LISTING MATCH] buyer={buyer_pk}, listing={listing_id}, "
        f"seller_min={seller_min}, bid={bid_price}, trade={trade_price}"
    )

    # Persist trade
    write_trade_item(
        listing=listing,
        buyer_pk=buyer_pk,
        bid_price=bid_price,
        ask_price=seller_min,
        trade_price=trade_price,
        now=now,
    )

    write_trade_notifications(
        listing_item=listing,
        buyer_pk=buyer_pk,
        trade_price=Decimal(str(trade_price)),
        now_iso=now.isoformat(),
    )

    # Update listing
    table.update_item(
        Key={"PK": listing["PK"], "SK": listing["SK"]},
        UpdateExpression=(
            "SET #st = :ended, "
            "MatchedBuyerPK = :b, "
            "MatchedTradePrice = :tp, "
            "MatchedAt = :now, "
            "CurrentHighestBid = :cbid, "
            "CurrentHighestBidderPK = :b "
        ),
        ExpressionAttributeNames={"#st": "Status"},
        ExpressionAttributeValues={
            ":ended": "ended",
            ":b": buyer_pk,
            ":tp": Decimal(str(trade_price)),
            ":now": now.isoformat(),
            ":cbid": Decimal(str(bid_price)),
        },
    )

    # Update bid
    table.update_item(
        Key={"PK": bid_item["PK"], "SK": bid_item["SK"]},
        UpdateExpression="SET BidStatus = :s",
        ExpressionAttributeValues={":s": "filled"},
    )

    print("[NEW_LISTING MATCH] Match persisted to DynamoDB.")


def is_within_auction_window(listing: dict, now: datetime) -> bool:
    """
    Returns True if 'now' is between AuctionStartsAt and AuctionEndsAt.
    If timestamps are missing or malformed, be conservative and return False.
    """
    start_str = listing.get("AuctionStartsAt")
    end_str = listing.get("AuctionEndsAt")

    try:
        if start_str:
            start = datetime.fromisoformat(start_str)
            if now < start:
                return False
        if end_str:
            end = datetime.fromisoformat(end_str)
            if now > end:
                return False
    except Exception as e:
        print("Error parsing auction window:", e, start_str, end_str)
        return False

    return True


def write_trade_item(
    listing: dict,
    buyer_pk: str,
    bid_price: float,
    ask_price: float,
    trade_price: float,
    now: datetime,
) -> None:
    """
    Write a TRADE item alongside the listing for history and reporting.

    This function does not update the listing or bid status.
    That is handled by the caller.
    """
    listing_pk = listing["PK"]
    market_key = listing.get("MarketKey")
    seller_pk = listing.get("SellerPK")

    trade_item = {
        "PK": listing_pk,
        "SK": "TRADE",
        "Type": "TRADE",
        "MarketKey": market_key,
        "ListingPK": listing_pk,
        "SellerPK": seller_pk,
        "BuyerPK": buyer_pk,
        "AskPrice": Decimal(str(ask_price)),
        "BidPrice": Decimal(str(bid_price)),
        "TradePrice": Decimal(str(trade_price)),
        "MatchedAt": now.isoformat(),
        "SettlementStatus": "pending_payment",
    }

    table.put_item(Item=trade_item)


def write_trade_notifications(listing_item, buyer_pk: str, trade_price: Decimal, now_iso: str) -> None:
    """
    Create DynamoDB notification items for buyer and seller,
    and optionally publish to Amazon Simple Notification Service.
    """
    listing_pk = listing_item["PK"]
    seller_pk = listing_item.get("SellerPK")
    device_pk = listing_item.get("DevicePK")
    market_key = listing_item.get("MarketKey")
    auction_mode = listing_item.get("AuctionMode", "continuous")

    base_payload = {
        "Type": "trade_filled",
        "ListingPK": listing_pk,
        "DevicePK": device_pk,
        "MarketKey": market_key,
        "TradePrice": trade_price,
        "AuctionMode": auction_mode,
        "CreatedAt": now_iso,
        "Read": False,
    }

    if buyer_pk:
        table.put_item(
            Item={
                "PK": f"NOTIF#{buyer_pk}",
                "SK": f"TS#{now_iso}#TRADE#BUYER",
                "UserRole": "buyer",
                **base_payload,
            }
        )
    if seller_pk:
        table.put_item(
            Item={
                "PK": f"NOTIF#{seller_pk}",
                "SK": f"TS#{now_iso}#TRADE#SELLER",
                "UserRole": "seller",
                **base_payload,
            }
        )

    if SNS_TOPIC_ARN:
        try:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="DeviceLoop trade matched",
                Message=json.dumps(
                    {
                        "listingPk": listing_pk,
                        "devicePk": device_pk,
                        "marketKey": market_key,
                        "tradePrice": float(trade_price),
                        "auctionMode": auction_mode,
                        "buyerPk": buyer_pk,
                        "sellerPk": seller_pk,
                        "createdAt": now_iso,
                    },
                    default=str,
                ),
            )
        except Exception as e:
            print(f"[NOTIF] Failed to publish to Amazon Simple Notification Service: {e}")


def serialize_decimals(obj):
    if isinstance(obj, list):
        return [serialize_decimals(x) for x in obj]
    if isinstance(obj, dict):
        return {k: serialize_decimals(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj
