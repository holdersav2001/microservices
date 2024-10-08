from datetime import datetime, timedelta

class DateTimeUtils:
    @staticmethod
    def t_plus_to_iso(business_date: str, t_plus: str) -> str:
        """
        Convert a T+ time to an ISO format datetime string.
        
        :param business_date: The business date in 'YYYY-MM-DD' format
        :param t_plus: The T+ time in format 'T+HH:MM:SS'
        :return: ISO format datetime string
        """
        base_date = datetime.strptime(business_date, '%Y-%m-%d')
        hours, minutes, seconds = map(int, t_plus[2:].split(':'))
        delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)
        result_datetime = base_date + delta
        return result_datetime.isoformat()

# This file can contain shared utility functions or classes
def shared_function():
    return "This is a shared function"