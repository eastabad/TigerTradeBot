from datetime import datetime
from app import db


class AnalysisReport(db.Model):
    __tablename__ = 'analysis_report'

    id = db.Column(db.Integer, primary_key=True)
    report_date = db.Column(db.Date, nullable=False, unique=True, index=True)
    report_data = db.Column(db.JSON, nullable=True)
    summary_data = db.Column(db.JSON, nullable=True)
    ai_analysis_data = db.Column(db.JSON, nullable=True)
    health_score = db.Column(db.Integer, default=0)
    total_trades = db.Column(db.Integer, default=0)
    total_pnl = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f'<AnalysisReport {self.report_date} health={self.health_score}>'

    def to_dict(self):
        return {
            'id': self.id,
            'report_date': str(self.report_date),
            'health_score': self.health_score,
            'total_trades': self.total_trades,
            'total_pnl': self.total_pnl,
            'summary': self.summary_data,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }
