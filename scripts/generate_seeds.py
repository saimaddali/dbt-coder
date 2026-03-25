"""
Generate expanded seed CSVs for jaffle_shop_v2.

22 tables, ~1000 total rows. Realistic e-commerce data with:
- Referential integrity (foreign keys valid)
- NULLs in realistic places
- Multi-year date ranges (2022-2024)
- Edge cases: inactive customers, cancelled orders, pending refunds
"""

import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)  # Reproducible

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "rl_sandbox", "dbt_project", "seeds")
os.makedirs(OUT_DIR, exist_ok=True)

# --- Helpers ---

def write_csv(filename, rows, headers):
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    print(f"  {filename}: {len(rows)} rows")

def rand_date(start, end):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def rand_ts(start, end):
    return rand_date(start, end).strftime("%Y-%m-%d %H:%M:%S")

def rand_date_str(start, end):
    return rand_date(start, end).strftime("%Y-%m-%d")

START = datetime(2022, 1, 1)
END = datetime(2024, 6, 30)

# --- 1. raw_customers (50 rows) ---
first_names = ["Michael","Sarah","David","Emily","James","Maria","Robert","Jennifer","William","Lisa",
               "Thomas","Amanda","Christopher","Jessica","Daniel","Ashley","Matthew","Stephanie","Andrew","Nicole",
               "Joshua","Elizabeth","Brandon","Megan","Ryan","Rachel","Justin","Lauren","Kevin","Samantha",
               "Brian","Katherine","Tyler","Michelle","Nathan","Angela","Steven","Heather","Eric","Christina",
               "Jonathan","Rebecca","Mark","Laura","Patrick","Amber","Timothy","Danielle","Jason","Tiffany"]
last_names = ["Perez","Johnson","Kim","Chen","Wilson","Garcia","Taylor","Brown","Martinez","Anderson",
              "Lee","White","Harris","Clark","Lewis","Robinson","Walker","Young","Allen","King",
              "Wright","Scott","Green","Baker","Adams","Nelson","Hill","Campbell","Mitchell","Roberts",
              "Carter","Phillips","Evans","Turner","Torres","Parker","Collins","Edwards","Stewart","Flores",
              "Morris","Nguyen","Murphy","Rivera","Cook","Rogers","Morgan","Peterson","Cooper","Reed"]
customer_types = ["individual","individual","individual","business","business"]
referral_sources = ["organic","referral","paid_search","paid_social","sales_outbound","partner"]

customers = []
for i in range(1, 51):
    created = rand_date(START, datetime(2024, 1, 1))
    updated = rand_date(created, END)
    is_active = "true" if random.random() > 0.15 else "false"
    email_domain = random.choice(["example.com","company.co","bigcorp.com","startup.io","enterprise.com","agency.com","gmail.com"])
    phone = f"555-{random.randint(1000,9999)}" if random.random() > 0.1 else ""
    customers.append([
        i, first_names[i-1], last_names[i-1],
        f"{first_names[i-1].lower()}.{last_names[i-1].lower()}@{email_domain}",
        phone,
        created.strftime("%Y-%m-%d %H:%M:%S"),
        updated.strftime("%Y-%m-%d %H:%M:%S"),
        is_active,
        random.choice(customer_types),
        random.choice(referral_sources),
    ])
write_csv("raw_customers.csv", customers,
          ["id","first_name","last_name","email","phone","created_at","updated_at","is_active","customer_type","referral_source"])

# --- 2. raw_products (30 rows) ---
products_data = [
    ("Espresso Machine Pro","appliances","coffee",29900,18000,8.5,"APL-ESP-001"),
    ("Pour Over Kit","accessories","coffee",4500,2200,0.8,"ACC-POK-001"),
    ("Dark Roast Beans 1kg","consumables","coffee",2400,1200,1.0,"CON-DRB-001"),
    ("Light Roast Beans 1kg","consumables","coffee",2600,1400,1.0,"CON-LRB-001"),
    ("Coffee Grinder Deluxe","appliances","coffee",15000,9500,3.2,"APL-CGD-001"),
    ("Ceramic Mug Set (4)","accessories","drinkware",3200,1500,2.0,"ACC-CMS-001"),
    ("Travel Tumbler","accessories","drinkware",2800,1300,0.4,"ACC-TTM-001"),
    ("Cold Brew Maker","appliances","coffee",8500,5000,1.5,"APL-CBM-001"),
    ("Milk Frother","accessories","coffee",3500,1800,0.6,"ACC-MFR-001"),
    ("Decaf Blend 1kg","consumables","coffee",2200,1100,1.0,"CON-DCB-001"),
    ("Barista Scale","accessories","tools",5500,3200,0.3,"ACC-BSC-001"),
    ("Cleaning Kit","accessories","maintenance",1800,800,0.5,"ACC-CLK-001"),
    ("French Press Large","appliances","coffee",6500,3800,1.2,"APL-FPL-001"),
    ("Latte Art Pitcher","accessories","tools",2200,1000,0.3,"ACC-LAP-001"),
    ("Single Origin Ethiopian 500g","consumables","coffee",1800,900,0.5,"CON-SOE-001"),
    ("Single Origin Colombian 500g","consumables","coffee",1600,800,0.5,"CON-SOC-001"),
    ("Thermal Carafe","accessories","drinkware",4800,2400,1.1,"ACC-TCA-001"),
    ("Coffee Storage Canister","accessories","storage",2000,900,0.4,"ACC-CSC-001"),
    ("Water Filter Set","accessories","maintenance",3000,1500,0.2,"ACC-WFS-001"),
    ("Espresso Cups (6)","accessories","drinkware",2800,1200,1.5,"ACC-ECU-001"),
    ("Manual Grinder","appliances","coffee",7500,4500,0.8,"APL-MGR-001"),
    ("Coffee Subscription Box","consumables","subscription",4500,2500,2.0,"CON-CSB-001"),
    ("Drip Coffee Maker","appliances","coffee",12000,7000,4.0,"APL-DCM-001"),
    ("Reusable Filter Set","accessories","maintenance",1200,500,0.1,"ACC-RFS-001"),
    ("Coffee Table Book","accessories","lifestyle",3500,1800,1.5,"ACC-CTB-001"),
    ("Espresso Tamper","accessories","tools",4000,2000,0.5,"ACC-ETM-001"),
    ("Medium Roast Beans 1kg","consumables","coffee",2500,1300,1.0,"CON-MRB-001"),
    ("Iced Coffee Glasses (4)","accessories","drinkware",2600,1100,1.8,"ACC-ICG-001"),
    ("Descaling Solution","consumables","maintenance",1500,600,0.3,"CON-DSS-001"),
    ("Gift Card $50","gift_cards","gift",5000,5000,0.0,"GFT-050-001"),
]

products = []
for i, (name, cat, sub, price, cost, weight, sku) in enumerate(products_data, 1):
    created = rand_ts(START, datetime(2023, 6, 1))
    updated = rand_ts(datetime(2023, 6, 1), END)
    is_active = "true" if random.random() > 0.1 else "false"
    products.append([i, name, cat, sub, price, cost, is_active, created, updated, weight, sku])
write_csv("raw_products.csv", products,
          ["id","name","category","subcategory","unit_price","cost","is_active","created_at","updated_at","weight_kg","sku"])

# --- 3. raw_categories (8 rows) ---
categories = [
    [1, "Coffee Equipment", None],
    [2, "Appliances", 1],
    [3, "Accessories", 1],
    [4, "Consumables", None],
    [5, "Coffee Beans", 4],
    [6, "Maintenance", 4],
    [7, "Drinkware", 3],
    [8, "Gift Cards", None],
]
write_csv("raw_categories.csv", categories, ["id","name","parent_category_id"])

# --- 4. raw_warehouses (4 rows) ---
warehouses = [
    [1, "West Coast Hub", "Portland", "OR", 5000],
    [2, "East Coast Hub", "Newark", "NJ", 3000],
    [3, "Central Warehouse", "Dallas", "TX", 4000],
    [4, "Returns Processing", "Phoenix", "AZ", 1000],
]
write_csv("raw_warehouses.csv", warehouses, ["id","name","city","state","capacity"])

# --- 5. raw_employees (15 rows) ---
emp_roles = [
    ("Alex","Morgan","Operations Manager","operations"),
    ("Jordan","Lee","Customer Support Lead","support"),
    ("Casey","Rivera","Warehouse Manager","operations"),
    ("Taylor","Park","Marketing Director","marketing"),
    ("Riley","Chen","Sales Representative","sales"),
    ("Morgan","Davis","Support Agent","support"),
    ("Jamie","Nguyen","Support Agent","support"),
    ("Quinn","Baker","Fulfillment Specialist","operations"),
    ("Drew","Campbell","Data Analyst","analytics"),
    ("Avery","Scott","Content Creator","marketing"),
    ("Blake","Turner","Sales Representative","sales"),
    ("Cameron","Hall","Inventory Specialist","operations"),
    ("Dakota","Flores","Email Marketing","marketing"),
    ("Emerson","Patel","QA Specialist","operations"),
    ("Finley","Adams","CEO","executive"),
]
employees = []
for i, (fn, ln, role, dept) in enumerate(emp_roles, 1):
    hire = rand_date_str(datetime(2020, 1, 1), datetime(2024, 1, 1))
    employees.append([i, fn, ln, role, dept, hire])
write_csv("raw_employees.csv", employees, ["id","first_name","last_name","role","department","hire_date"])

# --- 6. raw_suppliers (10 rows) ---
suppliers_data = [
    ("Bean Origin Co","beans@beanorigin.com","Colombia",14),
    ("Ethiopian Direct","info@ethdirect.com","Ethiopia",21),
    ("Pacific Equipment","sales@pacequip.com","US",7),
    ("Ceramic Works","orders@ceramicworks.com","Japan",30),
    ("Steel & Glass Mfg","sg@steelglass.com","China",25),
    ("Filter Supply Inc","info@filtersupply.com","US",5),
    ("Artisan Roasters","wholesale@artisanroast.com","US",3),
    ("PackageCo","ship@packageco.com","US",2),
    ("Global Freight","logistics@globalfreight.com","US",1),
    ("Eco Packaging","hello@ecopack.com","Germany",18),
]
suppliers = []
for i, (name, contact, country, lead) in enumerate(suppliers_data, 1):
    suppliers.append([i, name, contact, country, lead])
write_csv("raw_suppliers.csv", suppliers, ["id","name","contact_email","country","lead_time_days"])

# --- 7. raw_orders (100 rows) ---
statuses = ["completed","completed","completed","completed","completed","shipped","shipped","cancelled","returned","processing"]
shipping_methods = ["standard","standard","standard","express","express","overnight"]
channels = ["web","web","web","mobile","mobile","phone","in_store"]
currencies = ["USD"]

orders = []
for i in range(1, 101):
    cust_id = random.randint(1, 50)
    order_date = rand_date_str(datetime(2022, 6, 1), datetime(2024, 6, 1))
    status = random.choice(statuses)
    amount = random.choice([2400, 3500, 5000, 7500, 8500, 10500, 12000, 15000, 18000, 21000, 27500, 29900, 32000, 41000, 45000])
    ship = random.choice(shipping_methods)
    discount = int(amount * random.choice([0, 0, 0, 0.05, 0.10, 0.15])) if status != "cancelled" else 0
    tax = int(amount * 0.08)
    channel = random.choice(channels)
    notes = random.choice(["", "", "", "", "loyalty discount applied", "bulk order", "corporate account", "gift order", "repeat customer"])
    orders.append([i, cust_id, order_date, status, amount, ship, discount, tax, "USD", channel, notes])
write_csv("raw_orders.csv", orders,
          ["id","customer_id","order_date","status","amount","shipping_method","discount_amount","tax_amount","currency","channel","notes"])

# --- 8. raw_order_items (200 rows) ---
order_items = []
item_id = 1
for order in orders:
    oid = order[0]
    num_items = random.choice([1, 1, 2, 2, 3, 4])
    used_products = set()
    for _ in range(num_items):
        pid = random.randint(1, 30)
        while pid in used_products:
            pid = random.randint(1, 30)
        used_products.add(pid)
        qty = random.choice([1, 1, 1, 2, 2, 3, 4])
        up = products_data[pid - 1][3]  # unit_price
        disc_pct = random.choice([0, 0, 0, 5, 10, 15])
        created = order[2] + " " + f"{random.randint(8,17):02d}:{random.randint(0,59):02d}:00"
        order_items.append([item_id, oid, pid, qty, up, disc_pct, created])
        item_id += 1
        if item_id > 200:
            break
    if item_id > 200:
        break
write_csv("raw_order_items.csv", order_items,
          ["id","order_id","product_id","quantity","unit_price","discount_pct","created_at"])

# --- 9. raw_payments (120 rows) ---
payment_methods = ["credit_card","credit_card","credit_card","bank_transfer","bank_transfer","gift_card","paypal"]
payment_statuses = ["success","success","success","success","success","pending","failed"]

payments = []
pay_id = 1
for order in orders[:100]:
    oid = order[0]
    order_amt = order[4]
    status = order[3]
    num_payments = 1 if random.random() > 0.15 else 2
    for p in range(num_payments):
        amt = order_amt if num_payments == 1 else order_amt // 2
        pmethod = random.choice(payment_methods)
        pstatus = "failed" if status == "cancelled" else random.choice(payment_statuses)
        created = order[2] + " " + f"{random.randint(8,17):02d}:{random.randint(0,59):02d}:00"
        updated = created  # same for simplicity
        proc_id = f"proc_{pay_id:04d}"
        is_refund = "false"
        payments.append([pay_id, oid, pmethod, amt, pstatus, created, updated, proc_id, is_refund])
        pay_id += 1
        if pay_id > 120:
            break
    if pay_id > 120:
        break
write_csv("raw_payments.csv", payments,
          ["id","order_id","payment_method","amount","status","created_at","updated_at","processor_id","is_refund"])

# --- 10. raw_addresses (60 rows) ---
cities_states = [
    ("Austin","TX","78701"),("Portland","OR","97201"),("Seattle","WA","98101"),("Denver","CO","80201"),
    ("San Francisco","CA","94105"),("Chicago","IL","60601"),("San Jose","CA","95101"),("Minneapolis","MN","55401"),
    ("Boston","MA","02101"),("Los Angeles","CA","90001"),("New York","NY","10016"),("Atlanta","GA","30301"),
    ("Miami","FL","33101"),("Phoenix","AZ","85001"),("Dallas","TX","75201"),("Nashville","TN","37201"),
    ("Philadelphia","PA","19101"),("Detroit","MI","48201"),("Raleigh","NC","27601"),("Salt Lake City","UT","84101"),
]
streets = ["Oak St","Elm Ave","Pine Rd","Maple Ln","Market St","River Dr","Startup Way","Lake Blvd",
           "Enterprise Dr","Agency Ave","Commerce Blvd","Innovation Ct","Tech Park","Main St","Broadway"]

addresses = []
addr_id = 1
for cust_id in range(1, 51):
    if random.random() > 0.2:  # most customers have at least shipping
        city, state, zip_code = random.choice(cities_states)
        street = f"{random.randint(100,999)} {random.choice(streets)}"
        created = customers[cust_id-1][5]
        addresses.append([addr_id, cust_id, "shipping", street, city, state, zip_code, "US", "true", created, created])
        addr_id += 1
    if random.random() > 0.5:  # some have billing too
        city, state, zip_code = random.choice(cities_states)
        street = f"{random.randint(100,999)} {random.choice(streets)}"
        created = customers[cust_id-1][5]
        addresses.append([addr_id, cust_id, "billing", street, city, state, zip_code, "US", "true", created, created])
        addr_id += 1
    if addr_id > 60:
        break
write_csv("raw_addresses.csv", addresses,
          ["id","customer_id","address_type","street","city","state","zip_code","country","is_default","created_at","updated_at"])

# --- 11. raw_refunds (25 rows) ---
refund_reasons = ["wrong_size","defective","customer_changed_mind","late_delivery","damaged_in_shipping","not_as_described"]
refund_statuses = ["approved","approved","approved","pending","denied"]
agents = ["agent_01","agent_02","agent_03","agent_04"]

refunds = []
# Pick orders with completed/returned status
refund_orders = [o for o in orders if o[3] in ("completed","returned","shipped")]
random.shuffle(refund_orders)
for i in range(1, 26):
    order = refund_orders[i-1]
    oid = order[0]
    reason = random.choice(refund_reasons)
    amt = int(order[4] * random.choice([0.5, 0.75, 1.0]))
    status = random.choice(refund_statuses)
    requested = rand_ts(datetime.strptime(order[2], "%Y-%m-%d"), END)
    processed = rand_ts(datetime.strptime(requested[:10], "%Y-%m-%d"), END) if status != "pending" else ""
    agent = random.choice(agents)
    refunds.append([i, oid, reason, amt, status, requested, processed, agent])
write_csv("raw_refunds.csv", refunds,
          ["id","order_id","reason","refund_amount","status","requested_at","processed_at","processed_by"])

# --- 12. raw_inventory (50 rows) ---
inventory = []
for i in range(1, 51):
    pid = ((i - 1) % 30) + 1  # cycle through products, some have multiple warehouse entries
    wid = random.randint(1, 3)  # warehouses 1-3 (not returns)
    qty = random.randint(0, 500)
    reorder = random.randint(10, 50)
    last_restock = rand_date_str(datetime(2024, 1, 1), END)
    updated = rand_ts(datetime(2024, 3, 1), END)
    inventory.append([i, pid, wid, qty, reorder, last_restock, updated])
write_csv("raw_inventory.csv", inventory,
          ["id","product_id","warehouse_id","quantity_on_hand","reorder_point","last_restock_date","updated_at"])

# --- 13. raw_promotions (20 rows) ---
promo_names = ["SPRING20","SUMMER15","WELCOME10","LOYALTY25","FLASH50","BOGO","HOLIDAY30",
               "FREEESHIP","NEWYEAR","BIRTHDAY15","VIP20","REFER10","CYBER40","BLACKFRI","MEMORIAL",
               "LABOR10","JULY4TH","THANKS20","XMAS25","EASTER15"]
promo_types = ["percentage","percentage","percentage","fixed_amount","free_shipping"]

promotions = []
for i in range(1, 21):
    name = promo_names[i-1]
    ptype = random.choice(promo_types)
    value = random.choice([500, 1000, 1500, 2000, 2500]) if ptype == "fixed_amount" else random.choice([5, 10, 15, 20, 25, 30, 40, 50])
    if ptype == "free_shipping":
        value = 0
    start = rand_date_str(datetime(2023, 1, 1), datetime(2024, 6, 1))
    end = rand_date_str(datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7), datetime.strptime(start, "%Y-%m-%d") + timedelta(days=90))
    max_uses = random.choice([50, 100, 200, 500, None])
    times_used = random.randint(0, max_uses or 100)
    min_order = random.choice([0, 0, 2500, 5000, 10000])
    is_active = "true" if random.random() > 0.4 else "false"
    promotions.append([i, name, ptype, value, start, end, max_uses or "", times_used, min_order, is_active])
write_csv("raw_promotions.csv", promotions,
          ["id","code","promotion_type","discount_value","start_date","end_date","max_uses","times_used","min_order_amount","is_active"])

# --- 14. raw_shipping (80 rows) ---
carriers = ["USPS","UPS","FedEx","DHL"]
ship_statuses = ["delivered","delivered","delivered","delivered","in_transit","in_transit","label_created"]

shipping = []
shipped_orders = [o for o in orders if o[3] in ("completed","shipped","returned")]
random.shuffle(shipped_orders)
for i in range(1, min(81, len(shipped_orders) + 1)):
    order = shipped_orders[i-1]
    oid = order[0]
    carrier = random.choice(carriers)
    tracking = f"{carrier[:2].upper()}{random.randint(100000000, 999999999)}"
    ship_date = rand_date_str(datetime.strptime(order[2], "%Y-%m-%d"), datetime.strptime(order[2], "%Y-%m-%d") + timedelta(days=3))
    status = random.choice(ship_statuses)
    if status == "delivered":
        deliver_date = rand_date_str(datetime.strptime(ship_date, "%Y-%m-%d") + timedelta(days=1),
                                     datetime.strptime(ship_date, "%Y-%m-%d") + timedelta(days=10))
    else:
        deliver_date = ""
    weight = round(random.uniform(0.5, 15.0), 1)
    cost = random.choice([599, 899, 1299, 1599, 1999, 2499])
    shipping.append([i, oid, carrier, tracking, ship_date, deliver_date, status, weight, cost])
write_csv("raw_shipping.csv", shipping,
          ["id","order_id","carrier","tracking_number","shipped_date","delivered_date","status","weight_kg","shipping_cost"])

# --- 15. raw_reviews (60 rows) ---
review_texts = [
    "Great product, exactly what I needed!",
    "Good quality but took a while to arrive.",
    "Not worth the price, disappointing.",
    "Excellent! Will buy again.",
    "Average product, nothing special.",
    "Love it! Perfect for my morning routine.",
    "Arrived damaged, had to request replacement.",
    "Best purchase I've made this year.",
    "Decent quality for the price point.",
    "Would recommend to friends and family.",
    "",  # some reviews have no text
    "Works as described.",
    "Exceeded expectations!",
    "Could be better, but acceptable.",
    "Five stars, no complaints.",
]

reviews = []
for i in range(1, 61):
    cust_id = random.randint(1, 50)
    pid = random.randint(1, 30)
    rating = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 15, 35, 35])[0]
    text = random.choice(review_texts)
    created = rand_ts(datetime(2023, 1, 1), END)
    is_verified = "true" if random.random() > 0.2 else "false"
    reviews.append([i, cust_id, pid, rating, text, created, is_verified])
write_csv("raw_reviews.csv", reviews,
          ["id","customer_id","product_id","rating","review_text","created_at","is_verified_purchase"])

# --- 16. raw_sessions (200 rows) ---
devices = ["desktop","desktop","desktop","mobile","mobile","tablet"]
session_channels = ["direct","organic_search","paid_search","social","email","referral"]

sessions = []
for i in range(1, 201):
    cust_id = random.randint(1, 50) if random.random() > 0.3 else ""  # 30% anonymous
    start = rand_date(datetime(2023, 1, 1), END)
    duration = timedelta(minutes=random.randint(1, 120))
    end = start + duration
    device = random.choice(devices)
    channel = random.choice(session_channels)
    sessions.append([i, cust_id, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"), device, channel])
write_csv("raw_sessions.csv", sessions,
          ["id","customer_id","session_start","session_end","device_type","channel"])

# --- 17. raw_page_views (300 rows) ---
pages = ["/","/ ","/products","/products/espresso","/products/grinders","/products/beans",
         "/cart","/checkout","/account","/account/orders","/about","/contact","/blog",
         "/promotions","/gift-cards","/reviews","/faq","/shipping-info"]

page_views = []
for i in range(1, 301):
    sid = random.randint(1, 200)
    session = sessions[sid - 1]
    s_start = datetime.strptime(session[2], "%Y-%m-%d %H:%M:%S")
    s_end = datetime.strptime(session[3], "%Y-%m-%d %H:%M:%S")
    view_ts = rand_ts(s_start, s_end)
    page = random.choice(pages)
    duration = random.randint(3, 300)
    page_views.append([i, sid, page, view_ts, duration])
write_csv("raw_page_views.csv", page_views,
          ["id","session_id","page_url","viewed_at","duration_seconds"])

# --- 18. raw_support_tickets (40 rows) ---
ticket_categories = ["order_issue","product_question","return_request","shipping_delay","billing","technical","feedback"]
priorities = ["low","medium","medium","high","urgent"]
ticket_statuses = ["open","open","in_progress","resolved","resolved","resolved","closed"]

tickets = []
for i in range(1, 41):
    cust_id = random.randint(1, 50)
    oid = random.randint(1, 100) if random.random() > 0.3 else ""
    cat = random.choice(ticket_categories)
    priority = random.choice(priorities)
    status = random.choice(ticket_statuses)
    created = rand_ts(datetime(2023, 1, 1), END)
    resolved = rand_ts(datetime.strptime(created[:10], "%Y-%m-%d"), END) if status in ("resolved","closed") else ""
    assigned_to = random.choice([2, 6, 7, ""])  # employee IDs for support staff
    tickets.append([i, cust_id, oid, cat, priority, status, created, resolved, assigned_to])
write_csv("raw_support_tickets.csv", tickets,
          ["id","customer_id","order_id","category","priority","status","created_at","resolved_at","assigned_employee_id"])

# --- 19. raw_returns (30 rows) ---
return_reasons = ["defective","wrong_item","not_as_described","changed_mind","damaged_in_shipping","too_late"]
conditions = ["new","like_new","used","damaged"]

returns = []
return_items = list(range(1, min(len(order_items) + 1, 200)))
random.shuffle(return_items)
for i in range(1, 31):
    oi_id = return_items[i-1]
    oi = order_items[oi_id - 1]
    reason = random.choice(return_reasons)
    condition = random.choice(conditions)
    refund_amt = int(oi[4] * oi[3] * random.choice([0.5, 0.75, 1.0]))
    requested = rand_ts(datetime(2023, 6, 1), END)
    processed = rand_ts(datetime.strptime(requested[:10], "%Y-%m-%d"), END) if random.random() > 0.2 else ""
    status = "processed" if processed else "pending"
    returns.append([i, oi_id, reason, condition, refund_amt, status, requested, processed])
write_csv("raw_returns.csv", returns,
          ["id","order_item_id","reason","condition","refund_amount","status","requested_at","processed_at"])

# --- 20. raw_subscriptions (25 rows) ---
plans = ["basic","basic","standard","standard","premium","premium","enterprise"]
sub_statuses = ["active","active","active","cancelled","paused","expired"]
mrr_by_plan = {"basic": 1500, "standard": 3500, "premium": 7500, "enterprise": 15000}

subscriptions = []
for i in range(1, 26):
    cust_id = random.randint(1, 50)
    plan = random.choice(plans)
    start = rand_date_str(datetime(2023, 1, 1), datetime(2024, 3, 1))
    status = random.choice(sub_statuses)
    if status in ("cancelled", "expired"):
        end = rand_date_str(datetime.strptime(start, "%Y-%m-%d") + timedelta(days=30),
                           datetime.strptime(start, "%Y-%m-%d") + timedelta(days=365))
    else:
        end = ""
    mrr = mrr_by_plan[plan]
    subscriptions.append([i, cust_id, plan, start, end, mrr, status])
write_csv("raw_subscriptions.csv", subscriptions,
          ["id","customer_id","plan","start_date","end_date","mrr","status"])

# --- 21. raw_campaigns (12 rows) ---
campaign_data = [
    ("Spring Sale 2023","email","2023-03-01","2023-03-31",5000,"completed"),
    ("Summer Collection Launch","social","2023-06-15","2023-07-15",8000,"completed"),
    ("Back to School","paid_search","2023-08-01","2023-09-15",12000,"completed"),
    ("Holiday Gift Guide","email","2023-11-15","2023-12-31",15000,"completed"),
    ("New Year Kickoff","social","2024-01-01","2024-01-31",6000,"completed"),
    ("Valentine's Day","email","2024-02-01","2024-02-14",3000,"completed"),
    ("Spring Refresh","paid_search","2024-03-15","2024-04-30",10000,"completed"),
    ("Mother's Day Special","social","2024-04-15","2024-05-12",4000,"completed"),
    ("Summer Sale 2024","email","2024-06-01","2024-06-30",7000,"active"),
    ("Loyalty Program Launch","email","2024-03-01","2024-12-31",20000,"active"),
    ("Influencer Partnership","social","2024-04-01","2024-09-30",15000,"active"),
    ("Google Shopping","paid_search","2024-01-01","2024-12-31",25000,"active"),
]

campaigns = []
for i, (name, channel, start, end, budget, status) in enumerate(campaign_data, 1):
    campaigns.append([i, name, channel, start, end, budget, status])
write_csv("raw_campaigns.csv", campaigns,
          ["id","name","channel","start_date","end_date","budget","status"])

# --- 22. raw_email_events (100 rows) ---
event_types = ["sent","sent","sent","opened","opened","clicked","bounced","unsubscribed"]

email_events = []
for i in range(1, 101):
    cust_id = random.randint(1, 50)
    campaign_id = random.randint(1, 12)
    camp = campaign_data[campaign_id - 1]
    event_type = random.choice(event_types)
    event_ts = rand_ts(datetime.strptime(camp[2], "%Y-%m-%d"), datetime.strptime(camp[3], "%Y-%m-%d"))
    email_events.append([i, cust_id, campaign_id, event_type, event_ts])
write_csv("raw_email_events.csv", email_events,
          ["id","customer_id","campaign_id","event_type","event_timestamp"])

# --- Summary ---
print(f"\nDone! Generated 22 seed CSVs in {OUT_DIR}")
total = (len(customers) + len(products) + len(categories) + len(warehouses) + len(employees) +
         len(suppliers) + len(orders) + len(order_items) + len(payments) + len(addresses) +
         len(refunds) + len(inventory) + len(promotions) + len(shipping) + len(reviews) +
         len(sessions) + len(page_views) + len(tickets) + len(returns) + len(subscriptions) +
         len(campaigns) + len(email_events))
print(f"Total rows: {total}")
