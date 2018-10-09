import logging
from collections import OrderedDict

from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.conf import settings
from django.db.models import Count
from django.core.cache import cache
from pandas import DataFrame

from apps.search.models import EmergencyModal
from apps.metadata.models import Hotel


logger = logging.getLogger(__name__)


def get_hotel_count():  # pragma: no cover
    hotels = cache.get('hotel_count', 0)
    if hotels == 0:
        hotels = Hotel.objects.count()
        cache.set('hotel_count', hotels, 604800)
    return hotels


def get_emergency_modal():  # pragma: no cover
    try:
        return EmergencyModal.objects.get()
    except (MultipleObjectsReturned, ObjectDoesNotExist):
        logger.warn('No modal displayed: expected only one instance of EmergencyModal')
        return None


def log_size(df, descriptor):
    """
    Log size of DataFrames at INFO level. See pandas FAQs for more explanation:
    http://pandas.pydata.org/pandas-docs/stable/faq.html

    Primarily interested in order of magnitude, so not worried about base-10 vs.
    base-2 when converting to megabytes.

    Args:
        df (DataFrame)
        descriptor (str): E.g. name of df, or 'initial output'
    """
    descriptor = 'Size of {}:'.format(descriptor)
    logger.info('{:45} {:>5.1f}Mb'.format(
        descriptor,
        df.memory_usage(deep=True).sum() / 1000000),)


def create_session_key(place_name, check_in, check_out, occupants, latitude, longitude, currency):
    """
    We use the key to find our stays object.
    """
    dimensions = OrderedDict([
        ('place_name', place_name),
        ('check_in_as_str', check_in),
        ('check_out_as_str', check_out),
        ('occupants', occupants),
        ('latitude', latitude),
        ('longitude', longitude),
        ('currency', currency),
    ])
    return '|'.join(dimensions.values())


def get_suggested_cities():  # pragma: no cover
    countries = (
        Hotel.objects.values('iso_alpha_2_country_code')
        .annotate(hotel_count=Count('hotel_id')))

    """
    states = (
        Hotel.objects.values('iso_alpha_2_country_code', 'state')
        .annotate(hotel_count=Count('hotel_id')))

    counties = (
        Hotel.objects.values('iso_alpha_2_country_code', 'state', 'county')
        .annotate(hotel_count=Count('hotel_id')))
    """

    # We don't want to construct switches for too many hotels at once. The
    # technical limit is 2,000 per request and might work if we constrain to
    # only switches within the same city.
    oversize_countries = (
        countries
        .filter(hotel_count__gte=settings.MAXIMUM_COUNTRY_HOTEL_COUNT)
        .values_list('iso_alpha_2_country_code', flat=True))

    """
    oversize_states = (
        states
        .filter(hotel_count__gte=settings.MAXIMUM_COUNTRY_HOTEL_COUNT)
        .values_list('iso_alpha_2_country_code', flat=True))

    oversize_counties = (
        counties
        .filter(hotel_count__gte=settings.MAXIMUM_COUNTRY_HOTEL_COUNT)
        .values_list('iso_alpha_2_country_code', flat=True))
    """

    # TODO: Extend country methodology to state + county
    country_city_counts = (
        Hotel.objects.filter(iso_alpha_2_country_code__in=oversize_countries)
        .values('iso_alpha_2_country_code', 'city')
        .annotate(hotel_count=Count('hotel_id')))

    country_city_counts = DataFrame(list(country_city_counts))
    country_city_counts.sort_values(
        ['iso_alpha_2_country_code', 'hotel_count'], ascending=False, inplace=True)

    suggested_cities = (
        country_city_counts
        .groupby('iso_alpha_2_country_code')
        .nth(list(range(settings.SUGGESTED_CITY_COUNT))))

    suggested_cities.reset_index(inplace=True)

    suggested_cities['rank'] = (
        suggested_cities.groupby('iso_alpha_2_country_code')['hotel_count']
        .rank(method='first', ascending=False)
        .astype('int'))

    suggested_cities = suggested_cities.pivot(
        index='iso_alpha_2_country_code', columns='rank', values='city')

    return suggested_cities.to_dict('index')
