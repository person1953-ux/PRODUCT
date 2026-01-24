#  REST API + UI + CHART
from flask import Blueprint, jsonify, render_template, request
from models import inventory_lots
from collections import defaultdict

inventory_bp = Blueprint("inventory", __name__)

# -------------------------
# HOME PAGE
# -------------------------
@inventory_bp.route("/")
def home():
    return render_template("index.html")


# -------------------------
# REST API
# -------------------------
@inventory_bp.route("/api/inventory", methods=["GET"])
def get_inventory():
    return jsonify(inventory_lots)


# -------------------------
# SEARCH + FILTER UI
# -------------------------
@inventory_bp.route("/inventory")
def inventory_page():
    q = request.args.get("q", "").upper()
    location = request.args.get("location", "").upper()

    results = inventory_lots

    if q:
        results = [item for item in results if q in item["partnumber"]]

    if location:
        results = [item for item in results if item["location"] == location]

    return render_template("inventory.html", lots=results, q=q, location=location)


# -------------------------
# CHART DASHBOARD
# -------------------------
@inventory_bp.route("/chart")
def chart_page():
    totals = defaultdict(int)

    for item in inventory_lots:
        totals[item["location"]] += item["quantity"]

    labels = list(totals.keys())
    values = list(totals.values())

    return render_template("chart.html", labels=labels, values=values)
