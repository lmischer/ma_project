if __name__ == '__main__':
    for which in ['openiti', 'shamela']:
        path_logs = '/results/' + which + '/logs/'
        path_logs.mkdir(parents=True, exist_ok=True)

        path_qm = '/results/' + which + '/qm/'
        for subdir in ['dates', 'dates/date_duplicates', 'dates/no_dates', 'samples']:
            path_sub = path_qm + subdir + '/'
            path_sub.mkdir(parents=True, exist_ok=True)

    for w in ['dates', 'gazetteer', 'wikidata']:
        path = '/results/' + w + '/'
        path.mkdir(parents=True, exist_ok=True)

