from bs4 import BeautifulSoup

class ActivityParser:

    def __init__(self, filename):
        with open(filename) as f:
            self.soup = BeautifulSoup(f.read(), 'html.parser')
            assert('Netflix' in self.soup.title)

    def parse(self):
        output = map(ActivityParser._parse_row, self.soup.find_all('li', class_='retableRow'))
        return list(output)

    @staticmethod
    def _parse_row(data):
        row = {
            'date': data.find('div').text,
            'netflix_id': data.find('a')['href'].split('/')[-1],
            'title': data.find('a').text,
            'movie': True
        }

        if 'Season' in row['title'] or 'Series' in row['title']:
            row['movie'] = False
            ep_data = row['title'].split('"')

            try:
                row['episode'] = ep_data[1::2][0] # Grab text in quotes
                row['season'] = ep_data[0].split(':')[-2].strip().split(' ')[-1] # Get season number, right before quoted text
                row['series'] = ep_data[0].split(':')[:-2][0] # Everything else is the series name
            except:
                print('Parsing error: ' + row['title'])

        return row
