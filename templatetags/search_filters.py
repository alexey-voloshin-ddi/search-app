from django import template


register = template.Library()


@register.filter
def no_trailing_comma(value):  # pragma: no cover
    """
    Removes a trailing comma, to do, fix original data source!
    """
    value = value.strip()
    if value.endswith(","):
        value = value[:-1]
    return value


@register.filter
def format_nights(night_count):
    """
    Return night or nights, for use in e.g. the search criteria summary
    """
    if night_count > 1:  # pragma: no cover
        nights = 'nights'
    else:
        nights = 'night'

    return nights


@register.filter
def format_night_range(hotel_night_count, prior_nights):
    """
    Format the night range for each hotel, e.g. Nights 1 - 5 or Nights 1 & 2.

    Note that empty (nan) entries cause columns to be stored as floats rather
    than ints, so explicitly convert as first step.
    """
    hotel_night_count = int(hotel_night_count)
    prior_nights = int(prior_nights)

    start_night = prior_nights + 1
    end_night = prior_nights + hotel_night_count

    if hotel_night_count == 1:
        night_range = 'Night {}'.format(start_night)
    elif hotel_night_count == 2:  # pragma: no cover
        night_range = 'Nights {} & {}'.format(start_night, end_night)
    else:  # pragma: no cover
        night_range = 'Nights {} - {}'.format(start_night, end_night)

    return night_range


@register.filter
def get_item(dictionary, key):

    return dictionary.get(key)


@register.filter
def get_long_lang_for_hotels(hotel, hotels):
    """
    Filter to get hotels latitude and longitude for map creaation
    :param hotel: current Hotel instance
    :param hotels: iterable of Hotels in combo
    :return: string with data for html
    """
    if len(hotels) == 2:
        current_hotel = hotels[0] if hotels[0].name == hotel.name else hotels[1]
        second_hotel = hotels[1] if hotels[0].name == hotel.name else hotels[0]
        return 'data-latitude1={} data-longitude1={} data-latitude2={} data-longitude2={}'.format(
            current_hotel.latitude, current_hotel.longitude,
            second_hotel.latitude, second_hotel.longitude
        )
    else:
        return 'data-latitude1={} data-longitude1={}'.format(hotel.latitude, hotel.longitude)
