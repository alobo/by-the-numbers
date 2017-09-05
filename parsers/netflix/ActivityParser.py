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
        date = data.find('div').text
        title = data.find('a').text
        netflix_id = data.find('a')['href'].split('/')[-1]
        return {'date': date, 'netflix_id': netflix_id, 'title': title}
