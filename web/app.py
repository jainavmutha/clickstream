from flask import Flask, render_template, request, redirect, url_for
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Kafka configuration
KAFKA_TOPIC = "ecommerce_clicks"
KAFKA_BROKER = "localhost:9092"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Get form data
        user_name = request.form.get("user_name")
        product = request.form.get("product")

        # Send data to Kafka
        click_event = {
            "user_id": user_name,
            "product": product,
            "timestamp": int(time.time() * 1000)  # Milliseconds
        }
        producer.send(KAFKA_TOPIC, value=click_event)
        producer.flush()

        return redirect(url_for("index"))

    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)