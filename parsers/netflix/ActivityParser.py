from bs4 import BeautifulSoup

class NetflixActivityParser:

    def __init__(self, filename):
        with open(filename) as f:
            self.soup = BeautifulSoup(f.read(), 'html.parser')
            assert('Netflix' in self.soup.title)

    def parse(self):
        output = map(NetflixActivityParser._parse_row, self.soup.find_all('li', class_='retableRow'))
        return list(output)

    @staticmethod
    def _parse_row(data):
        row = {
            'date': data.find('div').text,
            'netflix_id': data.find('a')['href'].split('/')[-1],
            'title': data.find('a').text,
            'movie': True
        }

        # Most of netflix tv show data is formatted as series:season X:episode
        if 'Season' in row['title'] or \
            'Series' in row['title'] or \
            'Chapter' in row['title'] or \
            row['title'].count(':') >= 2:

            row['movie'] = False
            ep_data = row['title'].replace('"', '').split(':')

            try:

                # Handle the case where there are more than 2 colons
                if (len(ep_data) != 3):
                    # Find the season seperator
                    for i, j in enumerate(ep_data):
                        if 'Season' in j or 'Series' in j or 'Chapter' in j:
                            idx_season = i
                            break

                    episode = ':'.join(ep_data[idx_season:])
                    season = ep_data[idx_season]
                    series = ':'.join(ep_data[0:idx_season])

                    # Recombine into a list of 3 so the regular parse flow can resume
                    ep_data = [series, season, episode]


                row['episode'] = ep_data[2].strip()
                row['season'] = ep_data[1].split(' ')[-1].strip() # Get season number, right before quoted text
                row['series'] = ep_data[0].strip()

            except Exception as e:
                print(e)
                print('Parsing error: ' + row['title'])

        return row
