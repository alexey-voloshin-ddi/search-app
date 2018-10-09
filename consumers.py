import json
import logging

from django.conf import settings

from rq import Queue
from channels.handler import AsgiRequest
from channels.sessions import channel_and_http_session

from apps.search import tasks


logger = logging.getLogger(__name__)


@channel_and_http_session
def ws_connect(message):
    request = AsgiRequest(message)
    session_key = request.COOKIES.get(settings.SESSION_COOKIE_NAME, None)
    message.channel_session['session_key'] = session_key

    message.reply_channel.send({
        "text": json.dumps({"status": "connected"})
    })


@channel_and_http_session
def ws_receive(message):  # pragma: no cover
    try:
        criteria = json.loads(message['text'])
    except ValueError:
        logger.debug("ws message isn't json text=%s", message['text'])
        return

    if criteria['action'] == 'search':
        queue = Queue(criteria['currency'], connection=settings.REDIS_CONNECTION)

        # source_market checked/set already in Results view but get used for tests
        criteria['source_market'] = message.http_session.get('source_market', 'UK')
        session_key = message.channel_session['session_key']
        reply_channel = message.reply_channel.name

        queue.enqueue(
            tasks.execute_search,
            args=(criteria, session_key, reply_channel),
        )
