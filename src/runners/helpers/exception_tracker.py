import pybrake

from ..config import ENV, AIRBRAKE_PROJECT_KEY, AIRBRAKE_PROJECT_ID


class ExceptionTracker:
    airbrake_notifier = None

    def __init__(self):
        if AIRBRAKE_PROJECT_KEY and AIRBRAKE_PROJECT_ID:
            self.airbrake_notifier = pybrake.Notifier(
                project_id=AIRBRAKE_PROJECT_ID,
                project_key=AIRBRAKE_PROJECT_KEY,
                environment=ENV,
            )

    def notify(self, *args):
        for a in args:
            try:
                if self.airbrake_notifier:
                    self.airbrake_notifier.notify(a)
            except Exception as e:
                print(e)
