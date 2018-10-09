from django.core.management.base import BaseCommand
import logging
import os
from pandas import concat, datetime, date_range, DateOffset

from apps.search import tasks


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Run multiple searches and log the cheapest and best value switches"
    requires_migrations_checks = True

    base_data = {
        'data_mining': True,
        'source_market': 'UK',
        'place_name': '',
        'latitude': '0',
        'longitude': '0',
        'occupants': '2',
        'currency': 'gbp',
        'county': '',
    }

    base_check_in = datetime.strptime('2017-05-05', '%Y-%m-%d')
    check_in_range = date_range(base_check_in, base_check_in + DateOffset(days=60))

    cities = [{
        'city': 'New York',
        'state': 'NY',
        'country': 'US',
    }, {
        'city': 'Paradise',
        'state': 'NV',
        'country': 'US',
    }, {
        'city': 'Austin',
        'state': 'TX',
        'country': 'US',
    }, {
        'city': 'London',
        'state': 'England',
        'country': 'GB',
    }, {
        'city': 'Barcelona',
        'state': 'CT',
        'country': 'ES',
    }, {
        'city': 'Milan',
        'state': 'Lombardy',
        'country': 'IT',
    }, {
        'city': 'Shanghai',
        'state': 'Shanghai',
        'country': 'CN',
    }, {
        'city': 'Bangkok',
        'state': '',
        'country': 'TH',
    }, {
        'city': 'Singapore',
        'state': '',
        'country': 'SG',
    }, ]

    stay_durations = [3, 4, 5, 6]

    def handle(self, *args, **options):
        try:
            os.rename('analysis_output.csv', 'check_file_access.csv')
            os.rename('check_file_access.csv', 'analysis_output.csv')
        except OSError:
            raise Exception('Destination file is still open. Please close before running!')

        all_stays = []

        for city in self.cities:
            for check_in in self.check_in_range:
                for duration in self.stay_durations:
                    check_out = check_in + DateOffset(days=duration)
                    check_in_range = date_range(check_in, check_out - DateOffset(days=1))
                    data = self.base_data.copy()
                    data.update({
                        'checkIn': check_in,
                        'checkOut': check_out,
                        'check_in_range': check_in_range,
                        'country':  city['country'],
                        'state':  city['state'],
                        'city':  city['city'],
                    })

                    stays = tasks.execute_search(data, '', None)
                    result_count = len(stays)
                    if result_count == 0:
                        continue

                    stays.query('hotel_2_id != -1', inplace=True)
                    grouping_columns = ['primary_star_rating', 'min_review_tier']

                    stays.sort_values('stay_cost', inplace=True)
                    unrestricted_low_cost_stays = stays.groupby(grouping_columns).nth(0)
                    unrestricted_low_cost_stays['restricted'] = False

                    stays.sort_values('cost_per_quality_unit', inplace=True)
                    unrestricted_best_value_stays = stays.groupby(grouping_columns).nth(0)
                    unrestricted_best_value_stays['restricted'] = False

                    switches_with_both_benchmarks = \
                        'entire_stay_cost_1 == entire_stay_cost_1 \
                        and entire_stay_cost_2 == entire_stay_cost_2'

                    stays.query(switches_with_both_benchmarks, inplace=True)

                    stays.sort_values('stay_cost', inplace=True)
                    restricted_low_cost_stays = stays.groupby(grouping_columns).nth(0)
                    restricted_low_cost_stays['restricted'] = True

                    stays.sort_values('cost_per_quality_unit', inplace=True)
                    restricted_best_value_stays = stays.groupby(grouping_columns).nth(0)
                    restricted_best_value_stays['restricted'] = True

                    scenarios = [
                        unrestricted_low_cost_stays,
                        unrestricted_best_value_stays,
                        restricted_low_cost_stays,
                        restricted_best_value_stays, ]

                    stays = concat(scenarios)
                    stays.reset_index(inplace=True)
                    stays.drop_duplicates(inplace=True)

                    stays['city'] = city['city']
                    stays['check_in'] = check_in
                    stays['duration'] = duration
                    stays['result_count'] = result_count

                    all_stays.append(stays)
                    logger.warn('{}, {:%Y-%m-%d}, {}'.format(city['city'], check_in, duration))

        stays = concat(all_stays).to_csv('analysis_output.csv', index=False)
