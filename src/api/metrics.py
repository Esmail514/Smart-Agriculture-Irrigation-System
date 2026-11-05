"""Bounded HTTP telemetry: no farm, user, token, or raw URL labels."""
from time import perf_counter

from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

registry = CollectorRegistry()
requests_total = Counter('agri_api_requests_total', 'Completed API requests',
                         ['method', 'route', 'status'], registry=registry)
latency = Histogram('agri_api_request_duration_seconds', 'API request duration',
                    ['method', 'route'], buckets=(0.01, 0.05, 0.1, 0.3, 1, 3, 10, 30, 60), registry=registry)


def instrument(app):
    @app.middleware('http')
    async def record(request, call_next):
        start = perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get('route')
            label = getattr(route, 'path', 'unmatched')
            if label not in {'/metrics', '/health', '/health/live'}:
                method = request.method if request.method in {'GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD'} else 'OTHER'
                requests_total.labels(method, label, str(status)).inc()
                latency.labels(method, label).observe(perf_counter() - start)


def metrics_response():
    return Response(generate_latest(registry), headers={'Content-Type': CONTENT_TYPE_LATEST})
