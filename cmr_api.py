import requests
import pandas as pd
import datetime as dt
from shapely.geometry import MultiPolygon, Polygon, box
from tqdm import tqdm
from netrc import netrc
import os

# constants
PAGE_SIZE = 2000


doi = '10.5067/EMIT/EMITL2ARFL.001'# EMIT L2A Reflectance

# CMR API base url
cmrurl='https://cmr.earthdata.nasa.gov/search/' 

doisearch = cmrurl + 'collections.json?doi=' + doi
# get concept ID for DOI to retrieve download files
concept_id = requests.get(doisearch).json()['feed']['entry'][0]['id']

def getCmrFormattedDateRange(start_date, end_date):
	# CMR formatted start and end times
	dt_format = '%Y-%m-%dT%H:%M:%SZ'
	return start_date.strftime(dt_format) + ',' + end_date.strftime(dt_format)
# end def


def loadGranules(cmr_param):
	granule_arr = []

	while True:
		granulesearch = cmrurl + 'granules.json'
		response = requests.post(granulesearch, data=cmr_param)
		granules = response.json()['feed']['entry']
		
		if granules:
			for g in granules:
				granule_urls = ''
				granule_poly = ''
						
				# read cloud cover
				cloud_cover = g['cloud_cover']
		
				# reading bounding geometries
				if 'polygons' in g:
					polygons= g['polygons']
					multipolygons = []
					for poly in polygons:
						i=iter(poly[0].split (" "))
						ltln = list(map(" ".join,zip(i,i)))
						multipolygons.append(Polygon([[float(p.split(" ")[1]), float(p.split(" ")[0])] for p in ltln]))
					granule_poly = MultiPolygon(multipolygons)
				
				# Get https URLs to .nc files and exclude .dmrpp files
				granule_urls = [x['href'] for x in g['links'] if 'https' in x['href'] and '.nc' in x['href'] and '.dmrpp' not in x['href']]
				# Add to list
				granule_arr.append([granule_urls, cloud_cover, granule_poly])

			cmr_param["page_num"] += 1
		else: 
			break
	return granule_arr
# end def


def searchByPoint(lon, lat, page_num = 1):
	point_str = str(lon) +','+ str(lat)
 
	# Get search dates from user
	start_date = getDateInputFromUser("Please enter search start date (i.e. 2017,07,01)\n")
	end_date = getDateInputFromUser("Please enter search end date (i.e. 2017,08,20)\n")
	temporal_str = getCmrFormattedDateRange(start_date, end_date)
	
	# defining parameters
	cmr_param = {
		"collection_concept_id": concept_id, 
		"page_size": PAGE_SIZE,
		"page_num": page_num,
		"temporal": temporal_str,
		"point":point_str,
		# "cloud_cover": "0,10"
	}
 
	granule_arr = loadGranules(cmr_param)
 
	cmr_results_df = pd.DataFrame(granule_arr, columns=["asset_url", "cloud_cover", "granule_poly"])
	# Drop granules with empty geometry - if any exist
	cmr_results_df = cmr_results_df[cmr_results_df['granule_poly'] != '']
	# Expand so each row contains a single url 
	cmr_results_df = cmr_results_df.explode('asset_url')
	# Name each asset based on filename
	cmr_results_df.insert(0,'asset_name', cmr_results_df.asset_url.str.split('/',n=-1).str.get(-1))
	# Filter results by file name
	cmr_results_df = cmr_results_df[cmr_results_df.asset_name.str.contains('_RFL_')]

	return cmr_results_df


def getDateInputFromUser(prompt = 'Enter a date (i.e. 2017,7,1)\n'):
	date_entry = input(prompt)
	year, month, day = map(int, date_entry.split(','))
	return dt.datetime(year, month, day)

def download(url, fpath):
	"""Download a file from the given url to the target file path.
	Parameters
	----------
	url : str
		The url of the file to download.
	fpath : str
		The fully-qualified path where the file will be downloaded.
	"""
 
	urs = 'urs.earthdata.nasa.gov'
	
	try:
		netrcDir = os.path.expanduser("~/.netrc")
		netrc(netrcDir).authenticators(urs)[0]
	except FileNotFoundError:
		print('netRC file not found')
		exit()
	
	# Streaming, so we can iterate over the response.
	r = requests.get(url, verify=True, stream=True, auth=(netrc(netrcDir).authenticators(urs)[0], netrc(netrcDir).authenticators(urs)[2]))
	# r = requests.get(url, verify=False, stream=True, auth=("TheCrescentKing", "fXe6##gh9dSto@q#"))
	if r.status_code != 200:
		print("Error, file now downloaded.", r.reason)
		exit()
	# Total size in bytes.
	total_size = int(r.headers.get('content-length', 0))
	block_size = 1024  # 1 Kibibyte
	progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True)
	with open(fpath, 'wb') as f:
		for data in r.iter_content(block_size):
			progress_bar.update(len(data))
			f.write(data)
	progress_bar.close()
	if total_size != 0 and progress_bar.n != total_size:
		print("Error! Something went wrong during download.")


# Testing stuff

dataframe = searchByPoint(-62.1123, -39.89402)
print(dataframe)

url = dataframe.iloc[0]['asset_url']
filename = dataframe.iloc[0]['asset_name']

download(url, '../data/' + filename)