from channels import Channel
import django
import os
from django.conf import settings
from django.contrib.sessions.backends.db import SessionStore
from django.db import transaction
from django.db.models import F
import json
import logging
from math import ceil, floor
from pandas import DataFrame, datetime, date_range, DateOffset, melt
import pprint
import sys
import traceback
from urllib.request import unquote
from raven import Client
from raven.transport.http import HTTPTransport

# Required since the task is called "standalone" - see Django settings docs
django.setup()

from apps.apis.exceptions import RequestError, NoResultsError  # noqa
from apps.metadata.models import Hotel  # noqa
from apps.search import execute, utils  # noqa
from apps.search.models import LatestSaving  # noqa


logger = logging.getLogger(__name__)
client = Client(os.getenv('SENTRY_DSN', ''), transport=HTTPTransport)


def execute_search(criteria, session_key, reply_channel):
    run_from_management_command = criteria.get('data_mining')

    # Check-in range pre-calculated when running analytics
    if not run_from_management_command:  # pragma: no cover
        check_in = datetime.strptime(criteria['checkIn'], '%Y-%m-%d')
        check_out = datetime.strptime(criteria['checkOut'], '%Y-%m-%d')
        criteria['check_in_range'] = date_range(check_in, check_out - DateOffset(days=1))

    night_count = len(criteria['check_in_range'])

    outbound_message = {
        'status': '200',
        'currency': criteria['currency'],
        'currency_symbol': settings.CURRENCY_SYMBOLS[criteria['currency']],
        'country': criteria['country'],  # Blank if not country search
        'night_count': night_count,
    }

    try:
        criteria['city'] = unquote(criteria['city'])
        criteria['county'] = unquote(criteria['county'])
        criteria['state'] = unquote(criteria['state'])
        criteria['country'] = unquote(criteria['country'])

        if criteria['country'] in settings.BLOCKED_COUNTRIES:
            # We no longer permit searches for certain high-risk countries due
            # to high levels of attempted fraud. We block them in the front-end
            # but have this additional safeguard in case they are smart enough
            # to edit the URL directly (and another later in case they figure
            # out to submit a search without country parameter, but better to
            # catch them as early as possible)
            logger.error(
                'Someone tried searching for a blocked country {} via results URL'
                .format(criteria['country']))
            raise Exception

        _, stays = execute.search(criteria)

        if not run_from_management_command:  # pragma: no cover
            search_key = utils.create_session_key(
                unquote(criteria['place_name']),
                criteria['checkIn'],
                criteria['checkOut'],
                criteria['occupants'],
                criteria['latitude'],
                criteria['longitude'],
                criteria['currency'],
            )

            # Store complete record (including lengthy rateKey information) for
            # later use in stay detail view
            http_session = SessionStore(session_key=session_key)

            http_session[search_key] = {
                'stays': stays.to_json(),
                'timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            }

            http_session.save()

        fields_required_on_results_page = [
            'default_sort',
            'hotel_1_id',
            'check_in_1',
            'night_count_1',
            'entire_stay_cost_1',
            'hotel_2_id',
            'night_count_2',
            'entire_stay_cost_2',
            'switch_count',
            'distance_in_km',
            'rounded_stay_cost',
            'rounded_nightly_cost',
            'benchmark_stay_cost',
            'primary_star_rating',
            'review_score',
            'min_review_tier',
            'primary_review_tier',
            'refundable',
        ]

        required_fields_only_present_in_multi_night_search = [
            'check_in_2',
            'cost_delta_vs_stay_benchmark',
            'percentage_cost_delta_vs_stay_benchmark',
            'switch_benefit',
        ]

        if stays['switch_count'].max() > 0:  # pragma: no cover
            fields_required_on_results_page = \
                fields_required_on_results_page + required_fields_only_present_in_multi_night_search

            max_saving = abs(stays['percentage_cost_delta_vs_stay_benchmark'].min())
            if max_saving >= 0.3:
                log_max_saving(criteria, max_saving)

        if run_from_management_command:  # pragma: no cover
            # Hotel info not required; pass back to calling command
            fields_required_for_data_mining = ['stay_cost', 'cost_per_quality_unit']
            fields_required_on_results_page = \
                fields_required_on_results_page + fields_required_for_data_mining
            return stays[fields_required_on_results_page]

        outbound_message['stays'] = \
            stays[fields_required_on_results_page].to_json(orient='records')

        hotel_id_columns = stays.columns.str.contains('hotel_[\d]_id')
        hotel_ids = melt(stays.loc[:, hotel_id_columns]).dropna()['value'].unique()

        hotels = Hotel.objects.filter(hotel_id__in=hotel_ids).select_related().iterator()

        hotels = [{
            'hotel_id': str(hotel.hotel_id),  # String required for use as key
            'name': hotel.name,
            'star_rating': hotel.star_rating,
            'main_image_url': hotel.main_image_url,
            'recommendations': hotel.trustyou.recommendations,
            'summary': hotel.trustyou.summary,
            'trust_score': hotel.trustyou.trust_score,
            'trust_score_description': hotel.trustyou.trust_score_description,
            'review_count': hotel.trustyou.review_count,
            'category_badge': hotel.trustyou.category_badge,
            'latitude': hotel.latitude,
            'longitude': hotel.longitude,
        } for hotel in hotels]

        hotels = DataFrame(hotels)
        hotels.set_index('hotel_id', inplace=True)

        outbound_message['hotels'] = hotels.to_dict('index')

        min_stay_cost = stays['stay_cost'].min()
        max_stay_cost = stays['stay_cost'].max()
        try:  # pragma: no cover
            min_switch_distance = int(stays['distance_in_km'].min())
            max_switch_distance = int(stays['distance_in_km'].max())
        except ValueError:
            min_switch_distance = 0
            max_switch_distance = 0
        min_nightly_cost = min_stay_cost / night_count
        max_nightly_cost = max_stay_cost / night_count

        outbound_message['cost_ranges'] = {
            'minStayCost': floor(min_stay_cost),
            'maxStayCost': ceil(max_stay_cost),
            'minNightlyCost': floor(min_nightly_cost),
            'maxNightlyCost': ceil(max_nightly_cost),
        }

        outbound_message['distance_ranges'] = {
            'minDistanceSwitch': min_switch_distance,
            'maxDistanceSwitch': max_switch_distance,
        }

    except (RequestError, NoResultsError):
        error = 'RequestError or NoResultsError when searching for {}'.format(
            unquote(criteria['place_name'])
        )
        client.captureMessage(error)
        outbound_message['status'] = '503'
        logger.error(error)

        if run_from_management_command:  # pragma: no cover
            return DataFrame()

    except Exception:  # pragma: no cover
        outbound_message['status'] = '500'

        exception_type, _, exception_traceback = sys.exc_info()
        logger.error(exception_type)
        logger.error(pprint.pformat(traceback.format_tb(exception_traceback, limit=4)))

        if run_from_management_command:
            return DataFrame()

    if reply_channel is not None:  # pragma: no cover
        # This is actually tested but coverage cant detect it
        Channel(reply_channel).send({
            "text": json.dumps(outbound_message)
        })

    if outbound_message['status'] == '200':
        return True


def log_max_saving(criteria, max_saving, retain_count=5):  # pragma: no cover
    with transaction.atomic():
        LatestSaving.objects.select_for_update().all().update(position=F('position') + 1)

        short_place_name = unquote(criteria['place_name']).upper().split(',', 1)[0]
        if len(short_place_name) > 13:
            short_place_name = short_place_name[:10] + '...'  # pragma: no cover

        LatestSaving.objects.update_or_create(
            place_name=short_place_name,
            defaults={
                'position': 1,
                'check_in': datetime.strptime(criteria['checkIn'], '%Y-%m-%d'),
                'night_count': len(criteria['check_in_range']),
                'currency_symbol': settings.CURRENCY_SYMBOLS[criteria['currency']],
                'absolute_saving': 0,
                'percentage_saving': max_saving,
            })

        LatestSaving.objects.filter(position__gt=retain_count).delete()
