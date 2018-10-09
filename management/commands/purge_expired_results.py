from django.conf import settings
from django.contrib.sessions.backends.db import SessionStore
from django.contrib.sessions.models import Session
from django.core.management.base import BaseCommand
import logging
from pandas import datetime


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Remove expired session results"
    requires_migrations_checks = True

    def remove_expired_session_results(self):
        """
        We store result sets in the session variable for access from the stay
        detail view. They are valid for 30 minutes, after which they are purged
        by a scheduled Heroku call to this command.
        """
        maximum_result_age_in_seconds = settings.MAXIMUM_RESULT_AGE_IN_SECONDS

        session_keys = Session.objects.all().values_list('session_key', flat=True)

        for session_key in session_keys:
            session = SessionStore(session_key=session_key)

            # Each user session may have multiple associated search keys
            for key in list(session.keys()):
                item = session[key]

                # Identify keys associated with search results
                if type(item) != dict:
                    continue

                if sorted(list(item.keys())) == ['stays', 'timestamp']:
                    timestamp = datetime.strptime(item['timestamp'], '%Y-%m-%dT%H:%M:%S')
                    timedelta = datetime.now() - timestamp
                    age_in_seconds = timedelta.total_seconds()

                    if age_in_seconds > maximum_result_age_in_seconds:
                        del session[key]
                        session.save()

    def handle(self, *args, **options):
        self.remove_expired_session_results()
