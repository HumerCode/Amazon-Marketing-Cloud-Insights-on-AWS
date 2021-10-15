import json

from dateutil.parser import parse
from datetime import timedelta
from aws_lambda_powertools import Logger

logger = Logger(service="WorkflowLibrarySerice", level="INFO")


def get_date_spans_sliding(days, start_date, end_date):
    date_spans = []

    current_span_start_date = start_date
    current_span_end_date = current_span_start_date + timedelta(days=days)

    while current_span_end_date <= end_date:
        current_span = {}
        current_span["timeWindowStart"] = current_span_start_date.strftime('%Y-%m-%dT%H:%M:%S')
        current_span["timeWindowEnd"] = current_span_end_date.strftime('%Y-%m-%dT%H:%M:%S')
        current_span_start_date = current_span_start_date + timedelta(days=1)
        current_span_end_date = current_span_start_date + timedelta(days=days)
        date_spans.append(current_span)

    return (date_spans)


def get_date_spans_tumbling(days, start_date, end_date, allow_overlap):
    date_spans = []

    current_span_start_date = start_date
    current_span_end_date = current_span_start_date + timedelta(days=days)

    while current_span_end_date <= end_date:
        current_span = {}
        current_span["timeWindowStart"] = current_span_start_date.strftime('%Y-%m-%dT%H:%M:%S')
        current_span["timeWindowEnd"] = current_span_end_date.strftime('%Y-%m-%dT%H:%M:%S')
        current_span_start_date = current_span_start_date + timedelta(days=days)
        current_span_end_date = current_span_start_date + timedelta(days=days)
        date_spans.append(current_span)

    return (date_spans)


def lambda_handler(event, context):
    spans = []

    if 'days' not in event or 'startDate' not in event or 'endDate' not in event:
        message = 'The input event must contain days, startDate and endDate'
        logger.error(message)
        return ({'statusCode': 500, "message": message})

    start_date = parse(event['startDate'])
    end_date = parse(event['endDate'])

    if not event['days'] > 0 or not isinstance(event['days'], int):
        message = 'days value {} was invalid. The number of days must be an integer greater than 0'.format(
            event['days'])
        logger.error(message)
        return ({'statusCode': 500, "message": message})

    if not start_date < end_date:
        message = 'startDate {} must be less than than endDate {}'.format(start_date, end_date)
        logger.error(message)
        return ({'statusCode': 500, "message": message})

    if not 'windowType' in event:
        event['windowType'] = 'tumbling'

    dates_difference = end_date - start_date
    logger.info(
        'calcluated {} days between start_date {} and end_date {}'.format(dates_difference.days,
                                                                          event[
                                                                              'startDate'],
                                                                          event['endDate']))
    if event['windowType'].lower() == 'tumbling':
        if (dates_difference.days % event['days']) != 0:
            message = 'day interval {} is not possible with tumbling window and origin date {} and end date {} there would be a partial iteration {} required.'.format(
                event['days'], event['startDate'], event['endDate'],
                (dates_difference.days / event['days']))
            logger.error(message)
            return ({'statusCode': 500, "message": message})

    if 'windowType' not in event:
        logger.info('windowType was not specified in the event, setting the default value of "tumbling"')
        event['windowType'] = 'tumbling'

    if event['windowType'].lower() == 'sliding':
        spans = get_date_spans_sliding(event['days'], start_date, end_date)

    if event['windowType'].lower() == 'tumbling':
        if 'allow_tumbling_widow_overlap' not in event:
            logger.info('allow_tumbling_widow_overlap was not specified in the event, setting the default of False')
            event['allow_tumbling_widow_overlap'] = False
        spans = get_date_spans_tumbling(event['days'], start_date, end_date,
                                        event['allow_tumbling_widow_overlap'])

    # copy the values from the event into each item in the spans array
    for span in spans:
        if 'attributesToCopy' in event:
            for attribute in event['attributesToCopy']:
                span[attribute] = event['attributesToCopy'][attribute]
    logger.info(spans)
    logger.info('{} spans generated'.format(len(spans)))

    return spans