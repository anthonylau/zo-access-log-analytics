from urllib import request
import os
import gzip
from parse import compile
from datetime import datetime, timezone
import socket
from functools import lru_cache
import csv
import ipaddress
import bisect
import zipfile

log_parser = compile('{host} - - [{datetime:th}] {}')
from_date = datetime(1995, 8, 18, tzinfo=timezone.utc)
to_date = datetime(1995, 8, 21, tzinfo=timezone.utc)


def download_file_if_not_exists(file_url, file_name):
    if not os.path.isfile(file_name):
        print('Downloading data from', file_url)
        request.urlretrieve(file_url, filename=file_name)


def open_data():
    gz_file_name = 'NASA_access_log_Aug95.gz'
    data_file_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz'
    download_file_if_not_exists(data_file_url, gz_file_name)
    return gzip.open(gz_file_name, mode='rt', encoding='latin-1')


def logs():
    with open_data() as f:
        yield from f


def parse_log(log):
    return log_parser.parse(log)


@lru_cache(maxsize=100000)
def get_ip(host):
    try:
        return socket.gethostbyname(host)
    except socket.gaierror as e:
        return None


def ipaddress_range_country_items():
    ipv4_location_file_name = 'IP2LOCATION-LITE-DB1.CSV.ZIP'
    ipv4_location_file_url = 'http://download.ip2location.com/lite/IP2LOCATION-LITE-DB1.CSV.ZIP'
    ipv4_location_csv_file_name = 'IP2LOCATION-LITE-DB1.CSV'
    download_file_if_not_exists(ipv4_location_file_url, ipv4_location_file_name)
    if not os.path.isfile(ipv4_location_csv_file_name):
        print('Extracting', ipv4_location_csv_file_name)
        with zipfile.ZipFile(ipv4_location_file_name, "r") as zip_ref:
            zip_ref.extractall('.')

    with open(ipv4_location_csv_file_name) as f:
        reader = csv.reader(f)
        for row in reader:
            yield (int(row[0]), int(row[1]), row[2], row[3])


def create_get_country_func():
    """
    :return: ZZ if unknown
    """
    items = [(int(_[0]), int(_[1]), _[2], _[3]) for _ in ipaddress_range_country_items()]
    items = sorted(items, key=lambda x: x[0])
    from_ips = [int(i[0]) for i in items]

    def get_country(ip):
        ip_in_int = int(ipaddress.ip_address(ip))
        idx = bisect.bisect_left(from_ips, ip_in_int)
        if not idx:
            return 'ZZ'

        item = items[idx - 1]
        if not (item[0] <= ip_in_int <= item[1]):
            return 'ZZ'

        return item[2]

    return get_country


def main():
    get_country = create_get_country_func()
    total_number_of_requests = 0
    request_count_by_host = {}
    request_count_by_country = {}

    for log in logs():
        if not total_number_of_requests % 10000:
            print('Processed', total_number_of_requests)
        total_number_of_requests += 1

        entry = parse_log(log)
        host = entry['host']
        log_date = entry['datetime']
        ip = get_ip(host)
        country = get_country(ip) if ip else 'ZZ'  # if unable to resolve ip, treat as unknown
        country = country if country else 'ZZ'

        if from_date <= log_date < to_date:
            request_count_by_host[host] = request_count_by_host.get(host, 0) + 1

        request_count_by_country[country] = request_count_by_country.get(country, 0) + 1

    top_10_hosts = sorted(request_count_by_host.items(), key=lambda x: x[1], reverse=True)[:10]
    top_country = sorted(request_count_by_country.items(), key=lambda x: x[1], reverse=True)[:1]

    print('Total number of HTTP requests', total_number_of_requests)
    print('Top 10 most requests hosts from 18th Aug to 20th Aug', top_10_hosts)
    print('Country with most requests originating from (ZZ = UNKNOWN)', top_country)


if __name__ == "__main__":
    main()
