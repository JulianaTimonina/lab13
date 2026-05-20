import asyncio
import json
import os
from nats.aio.client import Client as NATS
from groq import Groq
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# OpenTelemetry setup
resource = Resource(attributes={SERVICE_NAME: "llm-agent"})
provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://jaeger:4317", insecure=True))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

client = Groq(api_key=os.environ["GROQ_API_KEY"])

async def main():
    nc = NATS()
    await nc.connect(servers=["nats://nats:4222"])

    async def message_handler(msg):
        with tracer.start_as_current_span("llm-explain") as span:
            data = json.loads(msg.data)
            decision = data.get("decision", {})
            client_info = data.get("client", {})

            prompt = (
                f"Ты – кредитный аналитик. Объясни заёмщику простыми словами, почему кредит "
                f"{'одобрен' if decision.get('approved') else 'не одобрен'}. "
                f"Сумма: {decision.get('amount')}, ставка: {decision.get('interest')}%. "
                f"Возраст: {client_info.get('age')}, доход: {client_info.get('income')}. "
                f"Причина: {decision.get('reason')}."
            )

            try:
                response = client.chat.completions.create(
                    messages=[{"role": "user", "content": prompt}],
                    model="llama-3.1-8b-instant",
                    temperature=0.3,
                )
                explanation = response.choices[0].message.content
            except Exception as e:
                explanation = f"Failed to generate explanation: {str(e)}"

            reply = {"explanation": explanation}
            await nc.publish(msg.reply, json.dumps(reply).encode())
            await msg.ack()

    await nc.subscribe("llm.explain.request", cb=message_handler, queue="llm-workers")
    print("LLM Agent started")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())