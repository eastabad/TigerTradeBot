import logging
import threading
from datetime import datetime, date, timedelta
from flask import render_template, request, jsonify, flash, redirect, url_for
from app import app

logger = logging.getLogger(__name__)

_running_analyses = {}


@app.route('/trade-analysis')
def trade_analysis_dashboard():
    from trade_analysis.models import AnalysisReport
    from datetime import date as d

    reports = AnalysisReport.query.order_by(AnalysisReport.report_date.desc()).limit(30).all()
    return render_template('trade_analysis.html', reports=reports, today=d.today().isoformat())


@app.route('/trade-analysis/report/<report_date>')
def trade_analysis_report(report_date):
    from trade_analysis.models import AnalysisReport

    try:
        target = datetime.strptime(report_date, '%Y-%m-%d').date()
    except ValueError:
        flash('Invalid date format', 'danger')
        return redirect(url_for('trade_analysis_dashboard'))

    report = AnalysisReport.query.filter_by(report_date=target).first()
    if not report:
        flash(f'No report found for {report_date}', 'warning')
        return redirect(url_for('trade_analysis_dashboard'))

    return render_template('trade_analysis_detail.html', report=report)


def _run_analysis_background(target, brokers, skip_ai, send_discord):
    date_key = str(target)
    try:
        _running_analyses[date_key] = 'running'
        from trade_analysis.orchestrator import run_daily_analysis
        with app.app_context():
            result = run_daily_analysis(
                target_date=target,
                brokers=brokers,
                skip_ai=skip_ai,
                send_discord=send_discord,
            )
            _running_analyses[date_key] = result.get('status', 'unknown')
            logger.info(f"Background analysis completed for {target}: {result.get('status')}")
    except Exception as e:
        logger.error(f"Background analysis failed for {target}: {e}", exc_info=True)
        _running_analyses[date_key] = f'error: {str(e)}'


@app.route('/trade-analysis/run', methods=['POST'])
def trade_analysis_run():
    target_date_str = request.form.get('target_date')
    skip_ai = request.form.get('skip_ai') == 'on'
    brokers_str = request.form.get('brokers', 'tiger,alpaca')

    if target_date_str:
        try:
            target = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        except ValueError:
            flash('Invalid date format', 'danger')
            return redirect(url_for('trade_analysis_dashboard'))
    else:
        target = date.today()

    brokers = [b.strip() for b in brokers_str.split(',') if b.strip()]

    date_key = str(target)
    if _running_analyses.get(date_key) == 'running':
        flash(f"Analysis for {target} is already running. Please wait.", 'info')
        return redirect(url_for('trade_analysis_dashboard'))

    t = threading.Thread(
        target=_run_analysis_background,
        args=(target, brokers, skip_ai, True),
        daemon=True
    )
    t.start()
    flash(f"Analysis started for {target}. It will run in the background (may take 1-2 minutes). Refresh this page to see results.", 'info')
    return redirect(url_for('trade_analysis_dashboard'))


@app.route('/api/trade-analysis/run', methods=['POST'])
def api_trade_analysis_run():
    data = request.get_json() or {}
    target_date_str = data.get('target_date')
    skip_ai = data.get('skip_ai', False)
    brokers = data.get('brokers', ['tiger', 'alpaca'])

    if target_date_str:
        try:
            target = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({'error': 'Invalid date format'}), 400
    else:
        target = date.today()

    date_key = str(target)
    if _running_analyses.get(date_key) == 'running':
        return jsonify({'status': 'already_running', 'message': f'Analysis for {target} is already running'})

    t = threading.Thread(
        target=_run_analysis_background,
        args=(target, brokers, skip_ai, True),
        daemon=True
    )
    t.start()
    return jsonify({'status': 'started', 'message': f'Analysis started for {target} in background'})


@app.route('/api/trade-analysis/status/<report_date>')
def api_trade_analysis_status(report_date):
    status = _running_analyses.get(report_date, 'idle')
    return jsonify({'date': report_date, 'status': status})
