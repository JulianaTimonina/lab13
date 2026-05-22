import json
import asyncio
import os
from nats.aio.client import Client as NATS
from groq import Groq
from opentelemetry import trace, propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# OpenTelemetry setup
resource = Resource(attributes={"service.name": "llm-agent"})
provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(
    OTLPSpanExporter(endpoint="http://jaeger:4317", insecure=True)
)
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

# Groq client
client = Groq(api_key=os.environ["GROQ_API_KEY"])

async def main():
    nc = NATS()
    await nc.connect("nats://nats:4222")

    async def handler(msg):
        print("LLM REQUEST RECEIVED")

        # 1. Извлекаем trace-контекст из заголовков
        headers = {}
        if msg.headers:
            headers = dict(msg.headers)

        ctx = propagate.extract(headers)  # работает с обычным dict

        # 2. Создаём дочерний спан
        with tracer.start_as_current_span(
            "llm-explain",
            context=ctx,
        ) as span:
            data = json.loads(msg.data.decode())
            decision = data.get("decision", {})
            client_info = data.get("client", {})

            prompt = (
                f"Ты – кредитный аналитик. Объясни заёмщику простыми словами, почему кредит "
                f"{'одобрен' if decision.get('approved') else 'не одобрен'}. "
                f"Сумма: {decision.get('amount')}, ставка: {decision.get('interest')}%. "
                f"Возраст: {client_info.get('age')}, доход: {client_info.get('income')}. "
                f"Причина: {decision.get('reason')}."
            )

            span.set_attribute("decision.approved", decision.get("approved"))
            span.set_attribute("client.age", client_info.get("age"))

            try:
                response = client.chat.completions.create(
                    messages=[{"role": "user", "content": prompt}],
                    model="llama-3.1-8b-instant",
                    temperature=0.3,
                )
                explanation = response.choices[0].message.content
            except Exception as e:
                explanation = f"Failed to generate explanation: {str(e)}"
                span.record_exception(e)

            result = {"explanation": explanation}
            response_bytes = json.dumps(result).encode()

            # 3. Инжектируем trace-контекст в ответ
            reply_headers = {}
            propagate.inject(reply_headers)

            if not msg.reply:
                print("ERROR: EMPTY REPLY SUBJECT")
                return

            await nc.publish(msg.reply, response_bytes, headers=reply_headers)
            print("RESPONSE SENT")

    await nc.subscribe("llm.explain.request", cb=handler)
    print("LLM agent started")
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())