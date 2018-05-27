from icalendar import Calendar, Event
import dateutil.rrule as rrule
from datetime import timezone
import pytz
import pandas as pd

class CalendarParser:

    def __init__(self, filename):
        with open(filename) as f:
            self.gcal = Calendar.from_ical(f.read())

    def parse(self):
        out = []
        for component in self.gcal.walk():
            if component.name == "VEVENT":

                # Ignore these events - added myself
                if ('PSYCH 256') in component.get('summary'): continue

                events = []
                info = CalendarParser._get_event_info(component.get('summary'))
                start = component.get('dtstart').dt
                end = component.get('dtend').dt
                duration = end - start

                # Ensure all timezones are UTC
                if (start.tzinfo != pytz.UTC):
                    start = start.astimezone(pytz.utc)
                    end = end.astimezone(pytz.utc)

                # Expand recurring rules
                if not component.get('rrule'):
                    events.append(start)
                else:
                    expand = rrule.rrulestr(component.get('rrule').to_ical().decode('utf-8'),
                                            dtstart=start)
                    events.extend(list(expand))

                for event in events:
                    out.append({
                        'course': ' '.join(info[0:2]),
                        'faculty': info[0].upper(),
                        'type': info[2].upper(),
                        'start': event,
                        'end': event + duration,
                        'duration': duration / pd.Timedelta('1 min') # convert to minutes
                    })
        return out

    @staticmethod
    def _get_event_info(event):
        if '-' in event: event = event.replace(' - ', ' ')
        event = event.split(' ')
        if (len(event) < 3): return None
        return (event[0], event[1], event[2])
