import argparse
import arrow
import json
import logging
import os

class Dumper():
    def __init__(self, config_fname):
        """ Initialize crawler with variables from config file"""

        with open(config_fname, 'r') as fp:
            self.config = json.load(fp)

        if 'log' in self.config:
            logging.basicConfig(filename=self.config['log'],
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            level=logging.WARN)

    def fname(self, date):
        """Construct the folder and filename for the given date and config file"""

        dump_folder = f"{self.config['dump_root']}/{startdate.year:04d}/{startdate.month:02d}/{startdate.day:02d}/"
        dump_fname = self.config['dump_fname']+'_'+startdate.format("YYYY-MM-DD")+'.csv'

        return dump_folder, dump_fname

    def dump(self, date, compress='lz4'):

        startdate = date
        enddate = date.shift(days=1)

        query = self.config['query'].format(startdate=startdate, enddate=enddate)
        dump_folder, dump_fname = self.fname(date)

        # create directories if needed
        os.makedirs(dump_folder, exist_ok=True) 

        cmd = """psql -d {db} -c "\copy ({query}) to '{fname}' csv header;" """.format(
            db=self.config['database'],
            query=query,
            fname=dump_folder+dump_fname
            )

        logging.debug(f'Dumping data to csv file ({cmd})...')
        ret_value = os.system( cmd )
        if ret_value != 0:
            logging.error(f'Could not dump data? Returned value: {ret_value}')
        
        if compress:
            cmd = f'{compress} {dump_folder}{dump_fname} {dump_folder}{dump_fname}.{compress}'
            logging.debug(f'Compressing data ({cmd})...')
            ret_value = os.system( cmd )
            os.remove(dump_folder+dump_fname)
            
        if ret_value != 0:
            logging.error(f'Could not compress data? Returned value: {ret_value}')

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
            description='Dump data from the database to a CSV file')
    parser.add_argument('--config', type=str, 
            help='configuration file with query and file structure details')
    parser.add_argument('--dates', default='', type=str, 
            help='file containing a list of dates to dump (one date per line)')
    parser.add_argument('--date', default='', type=str, 
            help='date to dump (e.g. 2022-01-20)')

    args = parser.parse_args()

    # Retrieve dates from file or set it to yesterday
    dates = []
    if args.dates:
        with open(args.dates, 'r') as fp:
            for date in fp.readlines():
                dates.append(arrow.get(date))
    elif args.date:
        dates.append(arrow.get(args.date))
    else:
        dates.append(arrow.utcnow().shift(days=-1))

    try:
        for date in dates:
            dumper = Dumper(config_fname=args.config)
            dumper.dump(date)

    # Log any error that could happen
    except Exception as e:
        logging.error('Error', exc_info=e)

