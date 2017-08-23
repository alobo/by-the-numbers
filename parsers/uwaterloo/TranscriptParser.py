import re

class TranscriptParser:

    def __init__(self, filename):
        with open(filename) as f:
            t = f.read()

        t = t.split('__________________________________________________________________________________________')
        self.header = t[0]
        self.semesters = t[1:]

    def parse(self):
        output = []
        for semester in self.semesters:
            if 'Full-Time' not in semester: continue # only care about full-time
            courses = parse_semester(semester)
            output.extend(courses)

        return output

    def _parse_semester(self, semester):
        lines = semester.split('\n')
        lines = list(filter(lambda x: len(x.strip()) > 0, lines))

        # Get the term information
        info = re.findall('([a-z,A-Z,0-9,-]+)', lines.pop(0))
        print(info)

        lines.pop(0) # Program info
        lines.pop(0) # Program info
        lines.pop(0) # Header

        courses = []
        for line in lines:
            line = list(filter(None, line.split(' ')))
            if 'Avg' in line: continue # ignore term stats
            if 'Units' in line: continue # ignore term stats
            if 'Term' in line: continue # ignore term stats

            if len(line) < 6:
                # This is a continuation of the previous course name
                courses[-1]['description'] += ' ' + line[0]
            else:
                desc_end = [i for i, x in enumerate(line) if '/' in x][0]
                courses.append({
                    'term': info[2],
                    'date': ' '.join(info[0:2]),
                    'course':       ' '.join(line[0:2]),
                    'description':  ' '.join(line[2:desc_end]),
                    'attempt/earn': line[desc_end],
                    'grade': int(line[desc_end + 1]) if line[desc_end + 1] != 'CR' else None,
                    'credit': line[desc_end + 2] == 'Y',
                    'in_gpa': line[desc_end + 3] == 'Y',
                })

        return courses
