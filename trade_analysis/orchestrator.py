import logging
from datetime import datetime, date, timedelta
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)


def run_daily_analysis(
    target_date: date = None,
    brokers: List[str] = None,
    skip_ai: bool = False,
    skip_signal_quality: bool = False,
    send_discord: bool = True,
) -> Dict:
    from trade_analysis.collector import collect_all_data
    from trade_analysis.matcher import run_matching
    from trade_analysis.ai_analyzer import run_ai_analysis
    from trade_analysis.reporter import generate_report, save_report_to_db, send_discord_notification

    if target_date is None:
        target_date = date.today()

    logger.info(f"=== Starting daily trade analysis for {target_date} ===")

    try:
        logger.info("Step 1: Collecting data...")
        all_data = collect_all_data(target_date, brokers=brokers)

        total_records = 0
        for broker_name, broker_data in all_data.items():
            for category, records in broker_data.items():
                total_records += len(records)
        logger.info(f"Total records collected: {total_records}")

        if total_records == 0:
            logger.info("No trading data found for this date. Skipping analysis.")
            return {
                'status': 'no_data',
                'date': str(target_date),
                'message': f'No trading data found for {target_date}',
            }

        logger.info("Step 2: Running rule-based matching...")
        match_results = run_matching(all_data)

        ai_analysis = {}
        if not skip_ai:
            logger.info("Step 3: Running AI analysis...")
            try:
                ai_analysis = run_ai_analysis(
                    match_results, all_data,
                    skip_signal_quality=skip_signal_quality
                )
            except Exception as e:
                logger.error(f"AI analysis failed, continuing without it: {e}")
                ai_analysis = {
                    'unmatched_analysis': {'summary': f'AI analysis failed: {str(e)}'},
                    'anomaly_analysis': {'summary': 'Skipped due to AI error'},
                    'signal_quality': {'summary': 'Skipped due to AI error'},
                }
        else:
            logger.info("Step 3: AI analysis skipped")

        logger.info("Step 4: Generating report...")
        report = generate_report(target_date, match_results, ai_analysis)

        logger.info("Step 5: Saving report to database...")
        report_id = save_report_to_db(report)

        if send_discord:
            logger.info("Step 6: Sending Discord notification...")
            send_discord_notification(report)

        logger.info(f"=== Daily analysis complete for {target_date}. Report ID: {report_id} ===")

        return {
            'status': 'success',
            'date': str(target_date),
            'report_id': report_id,
            'summary': report.get('summary', {}),
        }

    except Exception as e:
        logger.error(f"Daily analysis failed for {target_date}: {e}", exc_info=True)
        return {
            'status': 'error',
            'date': str(target_date),
            'error': str(e),
        }


def run_analysis_for_range(
    start_date: date,
    end_date: date,
    brokers: List[str] = None,
    skip_ai: bool = False,
) -> List[Dict]:
    results = []
    current = start_date
    while current <= end_date:
        result = run_daily_analysis(
            target_date=current,
            brokers=brokers,
            skip_ai=skip_ai,
            send_discord=False,
        )
        results.append(result)
        current += timedelta(days=1)
    return results
