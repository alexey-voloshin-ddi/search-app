from collections import OrderedDict
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.shortcuts import redirect
from django.views.generic import TemplateView
import logging
from pandas import DataFrame, DateOffset, datetime, date_range, merge, read_json

from apps.accounts import utils as account_utils
import apps.apis.tripadvisor.api as tripadvisor
from apps.apis.models import HotelbedsRoom, HotelbedsBoard, HotelbedsFacility
from apps.landing_pages.models import Event, Destination
from apps.metadata.models import Hotel
from apps.search import mixins, utils
from apps.search.models import LatestSaving


logger = logging.getLogger(__name__)


class Inputs(mixins.ContextSwitchMixin, TemplateView):
    template_name = 'search/homepage/inputs.html'

    def get_context_data(self, **kwargs):
        context = super(Inputs, self).get_context_data(**kwargs)
        request = self.request
        session = request.session
        user = request.user

        if 'source_market' not in session.keys():  # pragma: no cover
            account_utils.set_session_source_market(request)
        currency_code = account_utils.get_currency_from_source_market(
            request.session['source_market'])  # pragma: no cover

        emergency_modal = utils.get_emergency_modal()

        if 'offer' in self.request.path:
            context['offer'] = True

        if 'event_url' in kwargs.keys():  # pragma: no cover
            try:
                context['e'] = Event.objects.get(url=kwargs['event_url'])
            except ObjectDoesNotExist:
                pass  # Return regular homepage
        if 'destination_url' in kwargs.keys():  # pragma: no cover
            try:
                context['e'] = Destination.objects.get(url=kwargs['destination_url'])
                context['active_destination'] = True
            except ObjectDoesNotExist:
                pass  # Return regular homepage

        latest_savings = LatestSaving.objects.all()
        latest_savings = [
            '{:<15}{:%d/%m/%Y}{:>3}NTS - SAVE {:.0%}'.format(
                saving.place_name,
                saving.check_in,
                saving.night_count,
                saving.percentage_saving)
            for saving in latest_savings]

        context.update({
            'emergency_modal': emergency_modal,
            'currency_symbol': settings.CURRENCY_SYMBOLS[currency_code],
            'search_criteria': {
                'currency': currency_code,
            },
            'latest_savings': latest_savings,
            'hotel_count': utils.get_hotel_count(),  # pragma: no cover
            'blocked_countries': settings.BLOCKED_COUNTRIES,
            'google_maps_browser_api_key': settings.GOOGLE_MAPS_BROWSER_API_KEY,
            'google_analytics_tracking_id': settings.GOOGLE_ANALYTICS_TRACKING_ID,
        })

        last_search = None

        if user.is_authenticated:
            context['user_id'] = user.id
            last_search = user.last_search
        elif 'last_search' in session.keys():
            last_search = session['last_search']

        if last_search:
            context.update({
                'search_criteria': last_search
            })
            context['search_criteria']['currency'] = currency_code

        return context


class ResultView(TemplateView):
    """
    Abstraction of request and *kwargs handling for the Results and StayDetail
    views
    """
    def get_context_data(self, **kwargs):
        user = self.request.user
        context = super(ResultView, self).get_context_data(**kwargs)

        place_type = ''
        place_type_values = ['city', 'county', 'state', 'country']
        for value in place_type_values:
            if value in kwargs.keys():  # pragma: no cover
                place_type = value
                break

        search_criteria = {
            'location': {
                'place_name': kwargs['place_name'],
                'latitude': kwargs['latitude'],
                'longitude': kwargs['longitude'],
                'place_type': place_type,
                'country': kwargs.get('country', ''),
                'state': kwargs.get('state', ''),
                'county': kwargs.get('county', ''),
                'city': kwargs.get('city', ''),
            },
            'check_in': kwargs['check_in'],
            'check_out': kwargs['check_out'],
            'occupants': kwargs['occupants'],
            'currency': kwargs['currency'],
        }

        check_in = datetime.strptime(search_criteria['check_in'], '%Y-%m-%d')
        check_out = datetime.strptime(search_criteria['check_out'], '%Y-%m-%d')
        check_in_range = date_range(check_in, check_out - DateOffset(days=1))
        search_criteria['night_count'] = len(check_in_range)

        short_place_name = search_criteria['location']['place_name'].split(',', 1)[0]
        if len(short_place_name) > 13:
            search_criteria['short_place_name'] = short_place_name[:11] + '...'  # pragma: no cover
        else:
            search_criteria['short_place_name'] = short_place_name

        occupants = search_criteria['occupants'].split('-')
        search_criteria['adults'] = int(occupants.pop(0))
        search_criteria['children'] = len(occupants)

        context.update({
            'search_criteria': search_criteria,
            # Check-in and out passed as strings within search_criteria and as
            # dates at the context top-level
            'check_in': check_in,
            'check_out': check_out,
            'currency_symbol': settings.CURRENCY_SYMBOLS[search_criteria['currency']],
            'google_maps_browser_api_key': settings.GOOGLE_MAPS_BROWSER_API_KEY,
            'google_analytics_tracking_id': settings.GOOGLE_ANALYTICS_TRACKING_ID,
        })

        if user.is_authenticated:
            context['user_id'] = user.id

        return context


class Results(ResultView):
    """
    Results is as an empty shell. It requests and renders the actual search
    results via websocket/channels
    """
    template_name = 'search/results/results.html'

    def get_context_data(self, **kwargs):
        context = super(Results, self).get_context_data(**kwargs)
        context.update({
            'blocked_countries': settings.BLOCKED_COUNTRIES
        })
        request = self.request
        session = request.session
        user = request.user

        if 'source_market' not in session.keys():  # pragma: no cover
            account_utils.set_session_source_market(request)

        if user.is_authenticated:
            user.last_search = context['search_criteria']
            user.save()
        else:
            session['last_search'] = context['search_criteria']

        return context


class StayDetail(ResultView):
    template_name = 'search/stay_detail.html'

    def dispatch(self, request, *args, **kwargs):
        results_key = OrderedDict([
            ('place_name', kwargs['place_name']),
            ('check_in', kwargs['check_in']),
            ('check_out', kwargs['check_out']),
            ('occupants', kwargs['occupants']),
            ('latitude', kwargs['latitude']),
            ('longitude', kwargs['longitude']),
            ('currency', kwargs['currency']),
        ])

        results_key = '|'.join(results_key.values())

        try:
            stays = self.request.session.get(results_key)['stays']
        except TypeError:
            # No stays in session due to expired results or link-sharing.
            # TODO: Consider loading results page directly, but remember that
            # there is no access at this point to the additional location type
            # data (e.g. /US/NY//New%20York/) as this is stripped out of the
            # detail url (the same reason we don't set the user's last_search to
            # the requested search_criteria)
            return redirect('search:inputs')

        self.stays = read_json(
            stays,
            convert_dates=[
                'cancellation_deadline',
                'cancellation_deadline_1',
                'cancellation_deadline_2',
            ]
        )

        return super(StayDetail, self).dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super(StayDetail, self).get_context_data(**kwargs)
        stays = self.stays

        hotel_1_id = int(kwargs['hotel_1_id'])
        hotel_2_id = int(kwargs.get('hotel_2_id', 0))
        check_in_2 = self.kwargs.get('check_in_2')

        query = 'hotel_1_id == @hotel_1_id'

        max_switch_count = stays['switch_count'].max()

        if max_switch_count > 0:  # pragma: no cover
            query = query + \
                '& ((hotel_2_id == @hotel_2_id & check_in_2 == @check_in_2) \
                     | (@hotel_2_id == 0 & switch_count == 0))'

        stay = stays.query(query)

        check_out_1 = check_in_2 = datetime.strptime(stay['check_out_1'].values[0], '%Y-%m-%d')

        stay = stay.to_dict('records')[0]

        facilities = HotelbedsFacility.objects.all().iterator()

        facilities = [{
            'code': facility.code,
            'group': facility.group,
            'description': facility.description,
        } for facility in facilities]

        facilities = DataFrame(facilities)

        hotel = Hotel.objects.get(hotel_id=hotel_1_id)
        hotels = [hotel]

        try:  # pragma: no cover
            hotel_facilities = DataFrame(hotel.facilities)
            hotel_facilities.query('available == True', inplace=True)
            hotel_facilities = merge(hotel_facilities, facilities, on=['code', 'group'])

            hotel_facility_lists = [hotel_facilities.to_dict('records')]
        except Exception:
            hotel_facility_lists = []

        rooms = [HotelbedsRoom.objects.get(code=stay['room_type_1']).description]
        boards = [HotelbedsBoard.objects.get(code=stay['board_1']).description]
        if hotel_2_id > 0:  # pragma: no cover
            hotel = Hotel.objects.get(hotel_id=hotel_2_id)
            hotels.append(hotel)

            try:  # pragma: no cover
                hotel_facilities = DataFrame(hotel.facilities)
                hotel_facilities.query('available == True', inplace=True)
                hotel_facilities = merge(hotel_facilities, facilities, on=['code', 'group'])

                hotel_facility_lists.append(hotel_facilities.to_dict('records'))
            except Exception:
                pass

            rooms.append(HotelbedsRoom.objects.get(code=stay['room_type_2']).description)
            boards.append(HotelbedsBoard.objects.get(code=stay['board_2']).description)

        # Stored separately for passing to JS
        galleria_images = self.parse_hotel_images(hotels)

        try:  # pragma: no cover
            tripadvisor_reviews = [tripadvisor.get_tripadvisor_review(hotel.tripadvisor.tripadvisor)
                                   for hotel in hotels]
        except AttributeError:
            tripadvisor_reviews = []

        facilities = {}
        for idx, hotel in enumerate(hotels):
            lst = []
            try:
                lst = hotel_facility_lists[idx]
            except IndexError:
                pass
            facilities[hotel.hotel_id] = lst

        context.update({
            'stay': stay,
            'hotels': hotels,
            'galleria_images': galleria_images,
            'rooms': rooms,
            'boards': boards,
            'facilities': facilities,
            'check_out_1': check_out_1,
            'check_in_2': check_in_2,
            'tripadvisor_reviews': tripadvisor_reviews,
            'blocked_countries': settings.BLOCKED_COUNTRIES
        })

        return context

    def parse_hotel_images(self, hotels):
        hotelbeds_root = 'https://photos.hotelbeds.com/giata/bigger/'
        image_categories = ['GEN', 'HAB', 'COM', 'BAR', 'RES']

        galleria_images = []

        for hotel in hotels:
            images = DataFrame(hotel.images)

            images['imageTypeCode'] = images['imageTypeCode'].astype(
                'category', categories=image_categories, ordered=True)

            images.sort_values(['imageTypeCode', 'order'], inplace=True)

            images.rename(columns={'path': 'image'}, inplace=True)
            images['image'] = hotelbeds_root + images['image']
            images['thumb'] = images['image'].str.replace('bigger/', 'medium/')

            galleria_images.append(images[['image', 'thumb']].to_dict('records'))

        return galleria_images
