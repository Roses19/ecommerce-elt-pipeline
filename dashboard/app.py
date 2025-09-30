from flask import Flask, render_template

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html", title="Giới thiệu")

@app.route("/overview")
def overview():
    return render_template("overview.html", title="Tổng quan")

@app.route("/analysis")
def analysis():
    return render_template("delivery.html", title="Hiệu suất giao hàng")

@app.route("/customer")
def customer():
    return render_template("customer.html", title="Chất lượng khách hàng")

@app.route("/category")
def category():
    return render_template("category.html", title="Phân tích sản phẩm & danh mục")

@app.route("/seller")
def seller():
    return render_template("seller.html", title="Phân tích Seller")

@app.route("/payment")
def payment():
    return render_template("payment.html", title="Tài chính & Thanh toán")

if __name__ == "__main__":
    app.run(debug=True)
