import logging
from dataProcess import fetch_data


# Initiating and defining our logger.
logger = logging.getLogger(__name__)

logging.basicConfig(
    filename='pipeline.log',
    format='[%(asctime)s][%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%m:%S',
    level=logging.INFO
)

logger.info('starting the data collecting process')

# Starting the process.
print('starting the data collection process')

fetch_data()

