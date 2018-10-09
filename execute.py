from datetime import date  # NOQA - used when specifying check-in date manually
from django.conf import settings
import logging
import os
from pandas import datetime

import apps.apis.datafeeds as datafeeds
import apps.algorithm.switch_preparation as switch
import apps.algorithm.core as algorithm
import apps.algorithm.prepare_outputs as outputs
import apps.algorithm.filter_and_sort as filter_and_sort
from apps.search.utils import log_size


logger = logging.getLogger(__name__)


def search(criteria, supplier=None, display_all_columns=False):
    night_count = len(criteria['check_in_range'])

    if supplier is None:  # pragma: no cover
        supplier = settings.DEFAULT_SUPPLIER
    get_rates = getattr(datafeeds, 'get_' + supplier + '_rates')

    rates, entire_stay_costs = get_rates(criteria)
    log_size(rates, 'rates')
    log_size(entire_stay_costs, 'entire_stay_costs')

    rates = datafeeds.filter_out_unmapped_hotels(rates)

    max_switch_distance_in_km = settings.MAX_SWITCH_DISTANCE_IN_KM
    max_review_tier_decrease = settings.MAX_REVIEW_TIER_DECREASE

    # Paris is the largest city by number of hotels. While the default settings
    # work well for other cities, Paris requires some overrides
    if 2 < float(criteria['longitude']) < 2.6 \
            and 48.8 < float(criteria['latitude']) < 50:  # pragma: no cover
        logger.info('PARIS EXCEPTION APPLIED')
        max_switch_distance_in_km = int(os.getenv('PARIS_MAX_SWITCH_DISTANCE_IN_KM', 2))
        max_review_tier_decrease = int(os.getenv('PARIS_MAX_REVIEW_TIER_DECREASE', 0))

    start_time = datetime.now()
    switches = switch.construct_switches(
        criteria, entire_stay_costs, max_switch_distance_in_km, max_review_tier_decrease)

    end_time = datetime.now()
    elapsed_time = end_time - start_time
    logger.info('Switch construction took {}s'.format(elapsed_time))
    log_size(switches, 'switches')
    stays = algorithm.construct_stays(rates, criteria['check_in_range'], switches)
    log_size(stays, 'initial stays')

    stays = outputs.add_metadata_to_stays(stays)

    stays = outputs.add_benchmark_to_stays(stays)

    stays = filter_and_sort.filter_stays(
        stays, sample_rate=2, min_saving=-25, min_saving_percentage=-0.05,
        max_upgrade_cost=50, max_upgrade_cost_percentage=0.5)
    log_size(stays, 'filtered stays')

    stays = outputs.add_rate_information_to_stays(stays, rates)
    log_size(stays, 'filtered stays + rate info')

    stays = filter_and_sort.sort_stays(stays, night_count)
    stays = outputs.make_hotel_ids_int(stays)

    if stays['switch_count'].max() > 0:
        stays = outputs.add_switching_benefit(stays, criteria['currency'])

    if not display_all_columns:  # pragma: no cover
        stays = outputs.remove_no_longer_required_columns(stays)

    stays = outputs.round_data(stays)

    return rates, stays
