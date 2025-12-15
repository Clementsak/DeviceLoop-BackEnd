import os
import json
import logging
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

# Optional per-role topics (fall back to SNS_TOPIC_ARN if not provided)
NOTIFY_BUYER_SNS_ARN = os.environ.get("NOTIFY_BUYER_SNS_ARN") or SNS_TOPIC_ARN
NOTIFY_SELLER_SNS_ARN = os.environ.get("NOTIFY_SELLER_SNS_ARN") or SNS_TOPIC_ARN

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

EMAIL_FROM = os.environ["EMAIL_FROM"]
ses = boto3.client("ses", region_name=AWS_REGION)

def ddb_get_user_email(user_pk: str | None) -> str | None:
    if not user_pk:
        return None
    resp = table.get_item(Key={"PK": user_pk, "SK": "PROFILE"})
    item = resp.get("Item") or {}
    return item.get("Email")

def send_email(to_addr: str | None, subject: str, body: str) -> None:
    if not to_addr:
        logger.info("[EMAIL] skip: no recipient (subject=%s)", subject)
        return
    try:
        resp = ses.send_email(
            Source=EMAIL_FROM,
            Destination={"ToAddresses": [to_addr]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Text": {"Data": body, "Charset": "UTF-8"}},
            },
        )
        logger.info(
            "[EMAIL] sent ok to=%s subject=%s messageId=%s",
            to_addr,
            subject,
            resp.get("MessageId"),
        )
    except Exception as e:
        # Never break matching/clearing because email failed
        logger.exception("Email send failed: %s", e)


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    logger.info("[CLEARING] Running batch matching at %s", now.isoformat())

    # 1) Load all open, unexpired bids
    open_bids = load_open_bids(now)
    logger.info("[CLEARING] Loaded %d open bids", len(open_bids))

    # 2) Load all active listings that should be cleared now (interval + end_of_window)
    listings_interval, listings_eow = load_active_listings_to_clear(now)
    logger.info(
        "[CLEARING] Listings to clear: %d interval, %d end_of_window",
        len(listings_interval),
        len(listings_eow),
    )

    # 3) Group by market and mode
    markets: dict[str, dict[str, dict[str, list]]] = {}

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
        m["interval"]["bids"].append(bid)
        m["end_of_window"]["bids"].append(bid)

    # 4) Run double-auction matching per (market, mode)
    for mk, modes in markets.items():
        for mode_name, book in modes.items():
            asks = book["asks"]
            bids = book["bids"]

            # Helpful visibility: end_of_window should have asks after end time
            logger.info(
                "[CLEARING] Book market=%s mode=%s asks=%d bids=%d",
                mk,
                mode_name,
                len(asks),
                len(bids),
            )

            if not asks or not bids:
                # If end_of_window has asks but no bids, we still want run_double_auction_for_market()
                # to expire asks with notifications. But your current function expects bids too.
                # We'll handle "no bids" by calling it anyway (it will expire through no-cross path).
                if mode_name == "end_of_window" and asks and not bids:
                    run_double_auction_for_market(mk, mode_name, asks, bids, now)
                continue

            run_double_auction_for_market(mk, mode_name, asks, bids, now)

    logger.info("[CLEARING] Done matching step.")

    # Cleanup for continuous listings and stale bids (interval and end_of_window are not expired here)
    expire_stale_bids_and_listings(table, now, logger)

    return {"ok": True}


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    s = value.strip()
    # Convert "...Z" into a valid ISO-8601 offset for fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    dt = datetime.fromisoformat(s)

    # If no timezone info, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)



def expire_stale_bids_and_listings(table, now: datetime, logger):
    """
    Expire listings + bids for ALL auction modes, including continuous.

    Important:
    - Interval and end_of_window listings must NOT be expired here.
      They must be handled by the clearing logic (double auction step).
    """
    now_iso = now.isoformat()

    # EXPIRE LISTINGS (continuous only)
    try:
        resp = table.scan(FilterExpression=Attr("SK").eq("LISTING_REQUEST"))
        expired_listings = 0

        for item in resp.get("Items", []):
            status = item.get("Status") or item.get("status") or "active"
            mode = item.get("AuctionMode")

            # Do NOT expire interval or end-of-window listings here.
            if mode in ("interval", "end_of_window"):
                continue

            if status != "active":
                continue

            ends_at = parse_iso_utc(item.get("AuctionEndsAt"))
            if not ends_at or ends_at > now:
                continue

            table.update_item(
                Key={"PK": item["PK"], "SK": item["SK"]},
                UpdateExpression="""
                    SET #s = :expired,
                        EndReason = :reason,
                        EndedAt = :endedAt
                """,
                ExpressionAttributeNames={"#s": "Status"},
                ExpressionAttributeValues={
                    ":expired": "expired",
                    ":reason": "expired_no_match",
                    ":endedAt": now_iso,
                },
            )
            expired_listings += 1

            seller_pk = item.get("SellerPK")
            market_key = item.get("MarketKey") or ""

            if seller_pk:
                message = (
                    f"Your listing {item.get('PK')} in market {market_key} "
                    f"expired without any matching bid."
                )
                write_user_notification(
                    table=table,
                    user_pk=seller_pk,
                    notif_type="LISTING_EXPIRED_NO_MATCH",
                    message=message,
                )

            # --- DROP-IN: email for continuous listing expiry (no match) ---
            if seller_pk:
                seller_email = ddb_get_user_email(seller_pk)
                logger.info("[EMAIL] listing_expired_no_match seller_pk=%s seller_email=%s", seller_pk, seller_email)
                send_email(
                    seller_email,
                    "DeviceLoop listing expired without match",
                    (
                        f"Your listing expired without any matching bid.\n\n"
                        f"Listing: {item.get('PK')}\n"
                        f"Market: {market_key}\n"
                        f"Mode: continuous\n"
                        f"Ended at: {now_iso}\n"
                    ),
                )

            if SNS_TOPIC_ARN and seller_pk:
                payload = {
                    "type": "LISTING_EXPIRED_NO_MATCH",
                    "sellerPk": seller_pk,
                    "marketKey": market_key,
                    "listingPk": item.get("PK"),
                    "listingSk": item.get("SK"),
                    "endedAt": now_iso,
                }
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject="DeviceLoop listing expired with no match",
                    Message=json.dumps(payload),
                )

        if expired_listings:
            logger.info("[CLEANUP] Expired %d continuous listings", expired_listings)

    except Exception:
        logger.exception("[CLEANUP] Error while scanning for listings")


def load_open_bids(now: datetime) -> list[dict]:
    """
    Load all BID items with BidStatus='open'.
    Expire bids that passed BidExpiresAt, and write BID_EXPIRED notifications.
    """
    resp = table.scan(
        FilterExpression=Attr("SK").begins_with("BID") & Attr("BidStatus").eq("open")
    )
    items = resp.get("Items", []) or []

    open_bids: list[dict] = []

    for bid in items:
        expires_at_str = bid.get("BidExpiresAt")
        expires_at = None

        if expires_at_str:
            try:
                expires_at = parse_iso_utc(expires_at_str)
            except Exception:
                expires_at = None

        if expires_at and expires_at <= now:
            table.update_item(
                Key={"PK": bid["PK"], "SK": bid["SK"]},
                UpdateExpression="""
                    SET BidStatus = :expired,
                        ClosedReason = :reason,
                        ClosedAt = :closedAt
                """,
                ExpressionAttributeValues={
                    ":expired": "expired",
                    ":reason": "expired_timeout",
                    ":closedAt": now.isoformat(),
                },
            )

            buyer_pk = bid.get("BuyerPK")

            if SNS_TOPIC_ARN and buyer_pk:
                payload = {
                    "type": "BID_EXPIRED",
                    "buyerPk": buyer_pk,
                    "marketKey": bid.get("MarketKey"),
                    "bidPk": bid.get("PK"),
                    "bidSk": bid.get("SK"),
                    "expiresAt": expires_at_str,
                }
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject="DeviceLoop bid expired",
                    Message=json.dumps(payload),
                )

            if buyer_pk:
                buyer_email = ddb_get_user_email(buyer_pk)
                write_user_notification(
                    table=table,
                    user_pk=buyer_pk,
                    notif_type="BID_EXPIRED",
                    message=(
                        f"Your bid in market {bid.get('MarketKey','-')} has expired without a match. "
                        "You may place a new bid if you are still interested."
                    ),
                )
                logger.info("[EMAIL] bid_expired buyer_pk=%s buyer_email=%s", buyer_pk, buyer_email)

                send_email(
                    buyer_email,
                    "DeviceLoop bid expired",
                    (
                        f"Your bid expired without a match.\n\n"
                        f"Market: {bid.get('MarketKey','-')}\n"
                        f"Bid: {bid.get('PK')} / {bid.get('SK')}\n"
                        f"Expires at: {expires_at_str}\n"
                    ),
                )


            continue  # do NOT treat as open

        open_bids.append(bid)

    return open_bids


def load_active_listings_to_clear(now: datetime):
    """
    Returns two lists:
      - interval listings to be matched now (while still within the window)
      - end_of_window listings that have reached/passed AuctionEndsAt (FINAL match attempt)

    Critical fix:
      - end_of_window listings are NOT expired here.
        They are collected for final matching, and only expired by the matching step if no crossing occurs.
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
            start = parse_iso_utc(item.get("AuctionStartsAt"))
            end = parse_iso_utc(item.get("AuctionEndsAt"))

            if start and now < start:
                continue
            if not end:
                continue

            if mode == "interval":
                # Clear repeatedly while within window
                if now <= end:
                    listings_interval.append(item)
                else:
                    # Interval listing passed end time and still active -> expire it (no final-run requirement stated)
                    table.update_item(
                        Key={"PK": item["PK"], "SK": item["SK"]},
                        UpdateExpression="""
                            SET #s = :expired,
                                EndedAt = :endedAt,
                                EndReason = :reason
                        """,
                        ExpressionAttributeNames={"#s": "Status"},
                        ExpressionAttributeValues={
                            ":expired": "expired",
                            ":endedAt": now.isoformat(),
                            ":reason": "expired_no_match",
                        },
                    )
                    seller_pk = item.get("SellerPK")
                    market_key = item.get("MarketKey") or ""
                    if seller_pk:
                        write_user_notification(
                            table=table,
                            user_pk=seller_pk,
                            notif_type="LISTING_EXPIRED_NO_MATCH",
                            message=(
                                f"Your listing {item.get('PK')} in market {market_key} "
                                f"expired without any matching bid."
                            ),
                        )
                        seller_email = ddb_get_user_email(seller_pk)
                        logger.info("[EMAIL] interval_listing_expired seller_pk=%s seller_email=%s", seller_pk, seller_email)
                        send_email(
                            seller_email,
                            "DeviceLoop listing expired without match",
                            (
                                f"Your listing expired without any matching bid.\n\n"
                                f"Listing: {item.get('PK')}\n"
                                f"Market: {market_key}\n"
                                f"Ended at: {now.isoformat()}\n"
                            ),
                        )


            elif mode == "end_of_window":
                # FINAL run happens AFTER end time: include it for matching; do not expire here.
                if now >= end:
                    listings_eow.append(item)

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return listings_interval, listings_eow


def _num(v):
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def run_double_auction_for_market(
    market_key: str, mode: str, asks: list[dict], bids: list[dict], now: datetime
) -> None:
    """
    Double auction for one (marketKey, mode).

    Outcome:
      - Crossing exists -> one winning listing + one winning bid (matched)
      - No crossing -> expire all asks in this set (no_match) with seller notifications
    """
    buyer_topic_arn = SNS_TOPIC_ARN
    seller_topic_arn = SNS_TOPIC_ARN

    logger.info(
        "[CLEARING] Double auction market=%s mode=%s asks=%d bids=%d",
        market_key,
        mode,
        len(asks),
        len(bids),
    )

    if not asks:
        return

    priced_asks: list[tuple[float, dict]] = []
    for listing in asks:
        p = _num(listing.get("SellerMin") or listing.get("SellerMax"))
        if p is None:
            continue
        priced_asks.append((p, listing))

    priced_bids: list[tuple[float, dict]] = []
    for bid in bids:
        bp = (
            _num(bid.get("FinalBidPrice"))
            or _num(bid.get("BuyerMax"))
            or _num(bid.get("BidPrice"))
        )
        if bp is None:
            continue
        priced_bids.append((bp, bid))

    # If no valid prices or no bids -> treat as no match and expire asks (this is needed for end_of_window final run)
    if not priced_asks or not priced_bids:
        logger.info("[CLEARING] No price-valid asks/bids for market=%s mode=%s -> no match", market_key, mode)
        for listing in asks:
            _expire_listing_no_match(listing, market_key, mode, now, seller_topic_arn)
        return

    priced_asks.sort(key=lambda t: t[0])
    priced_bids.sort(key=lambda t: t[0], reverse=True)

    k_cross: int | None = None
    limit = min(len(priced_asks), len(priced_bids))
    for i in range(limit):
        ask_price, _ = priced_asks[i]
        bid_price, _ = priced_bids[i]
        if bid_price >= ask_price:
            k_cross = i

    if k_cross is None:
        logger.info("[CLEARING] No crossing for market=%s mode=%s -> expire asks", market_key, mode)
        for listing in asks:
            _expire_listing_no_match(listing, market_key, mode, now, seller_topic_arn)
        return

    ask_price, winning_listing = priced_asks[k_cross]
    bid_price, winning_bid = priced_bids[k_cross]
    trade_price = (ask_price + bid_price) / 2.0

    logger.info(
        "[CLEARING] MATCH market=%s mode=%s buyer=%s listing=%s trade=%.2f",
        market_key,
        mode,
        winning_bid.get("BuyerPK"),
        winning_listing.get("PK"),
        trade_price,
    )

    # Update winning listing
    table.update_item(
        Key={"PK": winning_listing["PK"], "SK": winning_listing["SK"]},
        UpdateExpression="""
            SET #s = :ended,
                EndedAt = :endedAt,
                CurrentTradePrice = :tradePrice,
                FinalMin = :finalMin,
                FinalMax = :finalMax,
                MatchedBuyerPK = :buyerPk,
                MatchedBidSK = :bidSk,
                PaymentStatus = :paymentPending
        """,
        ExpressionAttributeNames={"#s": "Status"},
        ExpressionAttributeValues={
            ":ended": "ended",
            ":endedAt": now.isoformat(),
            ":tradePrice": Decimal(str(trade_price)),
            ":finalMin": Decimal(str(ask_price)),
            ":finalMax": Decimal(str(ask_price)),
            ":buyerPk": winning_bid.get("BuyerPK"),
            ":bidSk": winning_bid.get("SK"),
            ":paymentPending": "pending",
        },
    )

    # Mark other listings as ended (lost)
    for _, listing in priced_asks:
        if listing is winning_listing:
            continue
        table.update_item(
            Key={"PK": listing["PK"], "SK": listing["SK"]},
            UpdateExpression="""
                SET #s = :ended,
                    EndedAt = :endedAt,
                    EndReason = :reason
            """,
            ExpressionAttributeNames={"#s": "Status"},
            ExpressionAttributeValues={
                ":ended": "ended",
                ":endedAt": now.isoformat(),
                ":reason": "lost_auction",
            },
        )

    # Update winning bid
    table.update_item(
        Key={"PK": winning_bid["PK"], "SK": winning_bid["SK"]},
        UpdateExpression=(
            "SET BidStatus = :s, "
            "MatchedListingPK = :lp, "
            "TradePrice = :tp, "
            "MatchedTradePrice = :tp, "
            "MatchedAt = :now"
        ),
        ExpressionAttributeValues={
            ":s": "matched",
            ":lp": winning_listing["PK"],
            ":tp": Decimal(str(trade_price)),
            ":now": now.isoformat(),
        },
    )

    # TRADE item + trade notifications
    buyer_pk = winning_bid.get("BuyerPK")
    if buyer_pk:
        write_trade_item(
            listing=winning_listing,
            buyer_pk=buyer_pk,
            bid_price=bid_price,
            ask_price=ask_price,
            trade_price=trade_price,
            now=now,
        )
        write_trade_notifications(
            listing_item=winning_listing,
            buyer_pk=buyer_pk,
            trade_price=Decimal(str(trade_price)),
            now_iso=now.isoformat(),
        )

    # Publish trade matched to Amazon Simple Notification Service
    payload = {
        "type": "TRADE_MATCHED",
        "marketKey": market_key,
        "mode": mode,
        "listingPk": winning_listing["PK"],
        "listingSk": winning_listing["SK"],
        "sellerPk": winning_listing.get("SellerPK"),
        "buyerPk": winning_bid.get("BuyerPK"),
        "bidPk": winning_bid.get("PK"),
        "bidSk": winning_bid.get("SK"),
        "tradePrice": trade_price,
        "paymentStatus": "pending",
    }
    message = json.dumps(payload)

    if seller_topic_arn and winning_listing.get("SellerPK"):
        sns.publish(
            TopicArn=seller_topic_arn,
            Subject="DeviceLoop trade matched",
            Message=message,
        )
    if buyer_topic_arn and winning_bid.get("BuyerPK"):
        sns.publish(
            TopicArn=buyer_topic_arn,
            Subject="DeviceLoop trade matched",
            Message=message,
        )


def _expire_listing_no_match(listing: dict, market_key: str, mode: str, now: datetime, seller_topic_arn: str | None):
    table.update_item(
        Key={"PK": listing["PK"], "SK": listing["SK"]},
        UpdateExpression="""
            SET #s = :expired,
                EndedAt = :endedAt,
                EndReason = :reason
        """,
        ExpressionAttributeNames={"#s": "Status"},
        ExpressionAttributeValues={
            ":expired": "expired",
            ":endedAt": now.isoformat(),
            ":reason": "no_match",
        },
    )

    seller_pk = listing.get("SellerPK")

    if seller_topic_arn and seller_pk:
        payload = {
            "type": "LISTING_EXPIRED_NO_MATCH",
            "sellerPk": seller_pk,
            "marketKey": market_key,
            "listingPk": listing["PK"],
            "listingSk": listing["SK"],
            "mode": mode,
        }
        sns.publish(
            TopicArn=seller_topic_arn,
            Subject="DeviceLoop listing expired with no match",
            Message=json.dumps(payload),
        )

    if seller_pk:
        message = (
            f"Your listing {listing.get('PK')} in market {market_key} "
            f"expired without any matching bid."
        )
        write_user_notification(
            table=table,
            user_pk=seller_pk,
            notif_type="LISTING_EXPIRED_NO_MATCH",
            message=message,
        )
        seller_email = ddb_get_user_email(seller_pk)
        logger.info("[EMAIL] eow_listing_expired seller_pk=%s seller_email=%s", seller_pk, seller_email)
        send_email(
            seller_email,
            "DeviceLoop listing expired without match",
            (
                f"Your listing expired without any matching bid.\n\n"
                f"Listing: {listing.get('PK')}\n"
                f"Market: {market_key}\n"
                f"Mode: {mode}\n"
                f"Ended at: {now.isoformat()}\n"
            ),
        )

def write_trade_item(
    listing: dict,
    buyer_pk: str,
    bid_price: float,
    ask_price: float,
    trade_price: float,
    now: datetime,
) -> None:
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
    listing_pk = listing_item["PK"]
    seller_pk = listing_item.get("SellerPK")
    device_pk = listing_item.get("DevicePK")
    market_key = listing_item.get("MarketKey")
    auction_mode = listing_item.get("AuctionMode", "continuous")

    base_payload = {
        "Type": "TRADE_MATCHED",
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

    buyer_email = ddb_get_user_email(buyer_pk)
    seller_email = ddb_get_user_email(seller_pk)

    buyer_body = (
        f"Your bid matched a listing.\n\n"
        f"Listing: {listing_pk}\n"
        f"Market: {market_key}\n"
        f"Trade price: {float(trade_price)}\n"
        f"Matched at: {now_iso}\n\n"
        f"Please log in to complete payment."
    )
    seller_body = (
        f"Your listing matched a bid.\n\n"
        f"Listing: {listing_pk}\n"
        f"Market: {market_key}\n"
        f"Trade price: {float(trade_price)}\n"
        f"Matched at: {now_iso}\n\n"
        f"Please log in to view the sale details."
    )

    send_email(buyer_email, "DeviceLoop trade matched", buyer_body)
    send_email(seller_email, "DeviceLoop trade matched", seller_body)


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
            logger.warning("[NOTIF] Failed to publish to Amazon Simple Notification Service: %s", e)


def write_user_notification(table, user_pk, notif_type, message: str):
    now_iso = datetime.now(timezone.utc).isoformat()
    table.put_item(
        Item={
            "PK": f"NOTIF#{user_pk}",
            "SK": f"TS#{now_iso}#EVENT#{notif_type}",
            "Type": notif_type,
            "Message": message,
            "CreatedAt": now_iso,
            "Read": False,
        }
    )
