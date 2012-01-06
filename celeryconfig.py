BROKER_URL = "amqp://guest:guest@localhost:5672//"
CELERY_RESULT_BACKEND = "amqp"
CELERY_IMPORTS = ('institution_searcher', )
CELERYD_CONCURRENCY = 20
