from django.conf.urls import include, url

from apps.search import views


app_name = 'search'

urlpatterns = [
    url(r'^$', views.Inputs.as_view(), name='inputs'),
    url(r'^offer/$', views.Inputs.as_view(), name='inputs'),
    # Check-in/-out expressed as YYYY-MM-DD
    # Lat/long rounded to 4dp; may include leading '-'
    url(r'^(?P<place_name>[^/]*)/'
        r'(?P<check_in>[0-9]{4}-[0-9]{2}-[0-9]{2})/'
        r'(?P<check_out>[0-9]{4}-[0-9]{2}-[0-9]{2})/'
        r'(?P<occupants>[^/]*)/'
        r'(?P<latitude>-?[0-9]*\.[0-9]*)/'
        r'(?P<longitude>-?[0-9]*\.[0-9]*)/'
        r'(?P<currency>[a-z]{3})/',
        include([
            url(r'^$',
                views.Results.as_view(),
                name='results'),
            url(r'^(?P<country>[A-Z]{2})/$',
                views.Results.as_view(),
                name='country_results'),
            url(r'^(?P<country>[A-Z]{2})/(?P<state>[^/]*)/$',
                views.Results.as_view(),
                name='state_results'),
            url(r'^(?P<country>[A-Z]{2})/(?P<state>[^/]*)/(?P<county>[^/]*)/$',
                views.Results.as_view(),
                name='county_results'),
            url(r'^(?P<country>[A-Z]{2})/(?P<state>[^/]*)/(?P<county>[^/]*)/(?P<city>[^/]*)/$',
                views.Results.as_view(),
                name='city_results'),

            url(r'^(?P<hotel_1_id>[0-9]*)/',
                include([
                    url(r'^$', views.StayDetail.as_view(), name='standard_stay'),
                    url(r'^(?P<hotel_2_id>[0-9]*)/'
                        r'(?P<check_in_2>[0-9]{4}-[0-9]{2}-[0-9]{2})/',
                        views.StayDetail.as_view(),
                        name='switching_stay')
                ]))
        ]))
]
