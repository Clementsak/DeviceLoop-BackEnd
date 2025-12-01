import os
import json
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
DDB_TABLE_NAME = os.environ["DDB_TABLE_NAME"]

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DDB_TABLE_NAME)

sns = boto3.client("sns")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    print(f"[CLEARING] Running batch matching at {now.isoformat()}")

    # 1) Load all open, unexpired bids
    open_bids = load_open_bids(now)
    print(f"[CLEARING] Loaded {len(open_bids)} open bids")

    # 2) Load all active listings that should be cleared now (interval + end_of_window)
    listings_interval, listings_eow = load_active_listings_to_clear(now)
    print(
        f"[CLEARING] Listings to clear: "
        f"{len(listings_interval)} interval, {len(listings_eow)} end_of_window"
    )

    # 3) Group by market and mode
    markets: dict[str, dict[str, dict[str, list]]] = {}

    # Helper to get / create per-market structure
    def ensure_market(mk: str):
        if mk not in markets:
            markets[mk] = {
                "interval": {"asks": [], "bids": []},
                "end_of_window": {"asks": [], "bids": []},
            }
        return markets[mk]

    # Add asks
    for listing in listings_interval:
        mk = listing.get("MarketKey")
        if not mk:
            continue
        m = ensure_market(mk)
        m["interval"]["asks"].append(listing)

    for listing in listings_eow:
        mk = listing.get("MarketKey")
        if not mk:
            continue
        m = ensure_market(mk)
        m["end_of_window"]["asks"].append(listing)

    # Add bids to both modes (an open bid can match interval or end_of_window)
    for bid in open_bids:
        mk = bid.get("MarketKey")
        if not mk:
            continue
        m = ensure_market(mk)
        # For simplicity we let bids compete in both pools.
        m["interval"]["bids"].append(bid)
        m["end_of_window"]["bids"].append(bid)

    # 4) Run double-auction matching per (market, mode)
    for mk, modes in markets.items():
        for mode_name, book in modes.items():
            asks = book["asks"]
            bids = book["bids"]
            if not asks or not bids:
                continue
            print(
                f"[CLEARING] Matching market={mk}, mode={mode_name}, "
                f"{len(asks)} asks, {len(bids)} bids"
            )
            run_double_auction_for_market(mk, mode_name, asks, bids, now)

    print("[CLEARING] Done.")
    return {"ok": True}


def load_open_bids(now: datetime) -> list[dict]:
    """Scan for bids with PK starting MARKET#, BidStatus=open, not expired."""
    bids: list[dict] = []
    scan_kwargs = {
        "FilterExpression": (
            Attr("PK").begins_with("MARKET#")
            & Attr("BidStatus").eq("open")
        )
    }

    last_key = None
    while True:
        if last_key:
            scan_kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            exp_str = item.get("BidExpiresAt")
            if not exp_str:
                continue
            try:
                exp = datetime.fromisoformat(exp_str)
                if now < exp:
                    bids.append(item)
            except Exception:
                continue
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return bids


def load_active_listings_to_clear(now: datetime):
    """
    Returns two lists:
      - interval listings to be matched now
      - end_of_window listings whose AuctionEndsAt <= now
    """
    listings_interval: list[dict] = []
    listings_eow: list[dict] = []

    scan_kwargs = {
        "FilterExpression": (
            Attr("SK").eq("LISTING_REQUEST")
            & Attr("Status").eq("active")
        )
    }

    last_key = None
    while True:
        if last_key:
            scan_kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**scan_kwargs)

        for item in resp.get("Items", []):
            mode = item.get("AuctionMode")
            start_str = item.get("AuctionStartsAt")
            end_str = item.get("AuctionEndsAt")

            try:
                if start_str:
                    start = datetime.fromisoformat(start_str)
                    if now < start:
                        continue
                end = datetime.fromisoformat(end_str) if end_str else None
            except Exception:
                continue

            if mode == "interval":
                # interval listing: active in window
                if end and now <= end:
                    listings_interval.append(item)
            elif mode == "end_of_window":
                # only clear after it ends
                if end and now >= end:
                    listings_eow.append(item)

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return listings_interval, listings_eow


def run_double_auction_for_market(
    market_key: str,
    mode_name: str,
    asks: list[dict],
    bids: list[dict],
    now: datetime,
) -> None:
    """
    Perform many-to-many double auction for one market + mode:

      - asks sorted by SellerMin ascending
      - bids sorted by BidPrice descending
      - while bid >= ask: trade at midpoint

    Updates DynamoDB for each match (TRADE item + listing + bid).
    """

    # Prepare asks with an explicit ask_price (SellerMin)
    ask_entries = []
    for s in asks:
        seller_min = s.get("SellerMin")
        if isinstance(seller_min, Decimal):
            seller_min = float(seller_min)
        if not isinstance(seller_min, (int, float)):
            continue

        final_min = s.get("FinalMin") or s.get("InitialMin")
        final_max = s.get("FinalMax") or s.get("InitialMax")
        final_min = float(final_min) if isinstance(final_min, (int, float, Decimal)) else None
        final_max = float(final_max) if isinstance(final_max, (int, float, Decimal)) else None

        ask_entries.append(
            {
                "raw": s,
                "seller_min": float(seller_min),
                "final_min": final_min,
                "final_max": final_max,
            }
        )

    if not ask_entries:
        return

    # Prepare bids
    bid_entries = []
    for b in bids:
        price = b.get("BidPrice")
        if isinstance(price, Decimal):
            price = float(price)
        if not isinstance(price, (int, float)):
            continue

        # Check expiry again just in case
        exp_str = b.get("BidExpiresAt")
        try:
            exp = datetime.fromisoformat(exp_str) if exp_str else None
            if exp and now >= exp:
                continue
        except Exception:
            continue

        bid_entries.append({"raw": b, "price": float(price)})

    if not bid_entries:
        return

    # Sort order books
    ask_entries.sort(key=lambda a: a["seller_min"])      # lowest ask first
    bid_entries.sort(key=lambda b: b["price"], reverse=True)  # highest bid first

    i = 0  # index in bids
    j = 0  # index in asks

    while i < len(bid_entries) and j < len(ask_entries):
        bid = bid_entries[i]
        ask = ask_entries[j]

        bid_price = bid["price"]
        ask_price = ask["seller_min"]

        if bid_price < ask_price:
            # market has cleared: best remaining bid is below best remaining ask
            break

        # Envelope check (platform FinalMin / FinalMax)
        fm = ask["final_min"]
        fx = ask["final_max"]
        if fm is not None and bid_price < fm:
            i += 1
            continue
        if fx is not None and bid_price > fx:
            i += 1
            continue

        trade_price = (bid_price + ask_price) / 2.0
        buyer_pk = bid["raw"].get("BuyerPK")
        listing = ask["raw"]
        print(
            f"[CLEARING] MATCH market={market_key}, mode={mode_name}, "
            f"buyer={buyer_pk}, listing={listing['PK']}, "
            f"bid={bid_price}, ask={ask_price}, trade={trade_price}"
        )

        # Persist match
        write_trade_item(
            listing=listing,
            buyer_pk=buyer_pk,
            bid_price=bid_price,
            ask_price=ask_price,
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
            Key={"PK": bid["raw"]["PK"], "SK": bid["raw"]["SK"]},
            UpdateExpression="SET BidStatus = :s",
            ExpressionAttributeValues={":s": "filled"},
        )

        i += 1
        j += 1  # move to next buyer and next seller


def write_trade_item(
    listing: dict,
    buyer_pk: str,
    bid_price: float,
    ask_price: float,
    trade_price: float,
    now: datetime,
) -> None:
    """
    Persist a TRADE record for an interval / end-of-window match.

    Listing + bid rows are updated separately in run_double_auction_for_market.
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

def write_trade_notifications(listing_item, buyer_pk: str, trade_price: Decimal, now_iso: str):
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

    # 1) DynamoDB notifications
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

    # 2) Amazon Simple Notification Service fan-out (optional)
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
