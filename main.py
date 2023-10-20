#
# Main program for photoapp program using AWS S3 and RDS to
# implement a simple photo application for photo storage and
# viewing.
#
# Authors:
#   Hye Won Hwang (hwh0745)
#   Northwestern University
#   Fall 2023
#

import datatier  # MySQL database access
import awsutil  # helper functions for AWS
import boto3  # Amazon AWS

import uuid
import pathlib
import logging
import sys
import os

from configparser import ConfigParser

import matplotlib.pyplot as plt
import matplotlib.image as img


###################################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number
  
  Parameters
  ----------
  None
  
  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """
  print()
  print(">> Enter a command:")
  print("   0 => end")
  print("   1 => stats")
  print("   2 => users")
  print("   3 => assets")
  print("   4 => download")
  print("   5 => download and display")
  print("   6 => upload")
  print("   7 => add user")

  cmd = int(input())
  return cmd


###################################################################
#
# Command 1: stats
#
def stats(bucketname, bucket, endpoint, dbConn):
  """
  Prints out S3 and RDS info: bucket name, # of assets, RDS 
  endpoint, and # of users and assets in the database
  
  Parameters
  ----------
  bucketname: S3 bucket name,
  bucket: S3 boto bucket object,
  endpoint: RDS machine name,
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  print("S3 bucket name:", bucketname)

  assets = bucket.objects.all()
  print("S3 assets:", len(list(assets)))

  #
  # MySQL info:
  #
  print("RDS MySQL endpoint:", endpoint)
  
  #
  # No. User info:
  #
  users_query = "SELECT COUNT(*) FROM users"
  users_cnt = datatier.retrieve_one_row(dbConn, users_query)
  if users_cnt:
    print("# of users:", users_cnt[0])

  # Retrieve and print number of assets from assets table
  assets_query = "SELECT COUNT(*) FROM assets"
  assets_cnt = datatier.retrieve_one_row(dbConn, assets_query)
  if assets_cnt:
    print("# of assets:", assets_cnt[0])
###################################################################
#
# Command 2: users
#
def users(dbConn):
  """
  Retrieves and outputs the users in the users table.
  The users are output in descending order by user id.
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  # Query for all users in users table in descending userid format
  list_user_query = "SELECT * FROM users ORDER BY userid DESC"
  users_ls = datatier.retrieve_all_rows(dbConn, list_user_query)

  # unpack query output in standardized print statement
  for user in users_ls:
    id = user[0]
    email = user[1]
    full_name =  user[2] + " , " + user[3]
    folder = user[4]
    print("User Id:", id)
    print("  Email:", email)
    print("  Name:", full_name)
    print("  Folder:", folder)
  
###################################################################
#
# Command 3: assets
#
def assets(dbConn):
  """
  Retrieves and outputs assets in the assets table.
  The assets are output in descending order by asset id.
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  # query for all assets in table in descending assetid format
  list_asset_query = "SELECT * FROM assets ORDER BY assetid DESC"
  assets_ls = datatier.retrieve_all_rows(dbConn, list_asset_query)
  # unpack query output to standardized print format
  for asset in assets_ls:
    asset_id, user_id, org_name, key_name = asset
    print("Asset id:", asset_id)
    print("  User id:", user_id)
    print("  Original name:", org_name)
    print("  Key name:", key_name)

###################################################################
#
# Command 4 & 5: download and display
#
def download(dbConn, bucket, display=False):
  """
  Inputs an asset id, and then looks up that asset in the database, 
  downloads the file, and renames it based on the original filename.
  
  Parameters
  ----------
  dbConn: open connection to MySQL server,
  bucket: S3 boto bucket object,
  display: if True, displays the downloaded image; default is False.

  Returns
  -------
  nothing
  """
  # get asset id from user
  print("Enter asset id>")
  asset_id = str(input())

  # get bucket key and asset name for the specific user
  file_info_query = "SELECT bucketkey, assetname FROM assets WHERE assetid = %s"
  file_info = datatier.retrieve_one_row(dbConn, file_info_query,[asset_id,])

  if file_info:
    # download file from bucket using info from query
    key, orginial_fname = file_info
    downloaded_file = awsutil.download_file(bucket,key,orginial_fname)
    # check if asset exists in bucket
    if downloaded_file:
      print("Downloaded from S3 and saved as ' ", orginial_fname, " '")
      # display asset if specified
      if display:
        image = img.imread(downloaded_file)
        plt.imshow(image)
        plt.show()
    else:
      print("No such asset...")
  else:
    print("No such asset...")

###################################################################
#
# Command 6: upload
#
def upload(dbConn, bucket):
  """
  Inputs the name of a local file, and a user id, and then uploads that file to the user’s folder in S3. 
  The file is given a unique name in S3 (use UUID module), and a row containing the asset’s information 
  --- user id, original filename, and full bucket key --- is inserted into the assets table
  
  Parameters
  ----------
  dbConn: open connection to MySQL server,
  bucket: S3 boto bucket object,

  Returns
  -------
  nothing
  """
  # Obtain local filename from user
  print("Enter local filename>")
  local_fname = str(input()) 
  # Check if local file exists
  if not os.path.exists(local_fname):
    print(f"Local file ' {local_fname} ' does not exist...")
    return
  
  # Obtain user id from user
  print("Enter user id>")
  user_id = str(input())
  # Check if user id exists
  check_user_query = "SELECT userid FROM users WHERE userid = %s"
  if not datatier.retrieve_one_row(dbConn, check_user_query, [user_id]):
    print("No such user...")
    return
  
  # Create uuid filename and upload to S3 bucket
  key = str(uuid.uuid4()) + pathlib.Path(local_fname).suffix
  s3_key = awsutil.upload_file(local_fname, bucket, key)
  if not s3_key:
    print("Failed to upload the file to S3.")
    return
  else:
    print(f"Uploaded and stored in S3 as ' {s3_key} '")

  # Insert upload data to RDS
  insert_query = "INSERT INTO assets(userid, assetname, bucketkey) VALUES (%s, %s, %s)"
  res = datatier.perform_action(dbConn, insert_query, [user_id, local_fname, s3_key])
  if res <= 0:
    print("Failed to insert asset info into the database.")
  else:
    # Get the last inserted asset id and print it
    asset_id_query = "SELECT LAST_INSERT_ID();"
    last_id = datatier.retrieve_one_row(dbConn, asset_id_query)
    print("Recorded in RDS under asset id ", last_id[0])

###################################################################
#
# Command 7: add user
#
def add_user(dbConn, bucket):
  """
  Inputs data about a new user and inserts a new row into the users table. 
  Input the new user’s email, last name, and first name.
  
  Parameters
  ----------
  dbConn: open connection to MySQL server,
  bucket: S3 boto bucket object,

  Returns
  -------
  nothing
  """
  # obtain user's email, last name and first name
  print("Enter user's email>")
  email = str(input())
  print("Enter user's last (family) name>")
  last_name = str(input())
  print("Enter user's first (given) name>")
  first_name = str(input())

  folder = str(uuid.uuid4())

  # insert user in users table as a new row
  insert_query = "INSERT INTO users(email, lastname, firstname, bucketfolder) VALUES (%s, %s, %s, %s)"
  res = datatier.perform_action(dbConn, insert_query, [email, last_name, first_name, folder])
  if res <= 0:
    print("Failed to insert user info into the database.")
  else:
    # Get the last inserted asset id and print it
    id_query = "SELECT LAST_INSERT_ID();"
    last_id = datatier.retrieve_one_row(dbConn, id_query)
    print("Recorded in RDS under user id ", last_id[0])

#########################################################################
# main
#
print('** Welcome to PhotoApp **')
print()

# eliminate traceback so we just get error message:
sys.tracebacklimit = 0

#
# what config file should we use for this session?
#
config_file = 'photoapp-config.ini'

print("What config file to use for this session?")
print("Press ENTER to use default (photoapp-config.ini),")
print("otherwise enter name of config file>")
s = input()

if s == "":  # use default
  pass  # already set
else:
  config_file = s

#
# does config file exist?
#
if not pathlib.Path(config_file).is_file():
  print("**ERROR: config file '", config_file, "' does not exist, exiting")
  sys.exit(0)

#
# gain access to our S3 bucket:
#
s3_profile = 's3readwrite'

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file

boto3.setup_default_session(profile_name=s3_profile)

configur = ConfigParser()
configur.read(config_file)
bucketname = configur.get('s3', 'bucket_name')

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucketname)

#
# now let's connect to our RDS MySQL server:
#
endpoint = configur.get('rds', 'endpoint')
portnum = int(configur.get('rds', 'port_number'))
username = configur.get('rds', 'user_name')
pwd = configur.get('rds', 'user_pwd')
dbname = configur.get('rds', 'db_name')

dbConn = datatier.get_dbConn(endpoint, portnum, username, pwd, dbname)

if dbConn is None:
  print('**ERROR: unable to connect to database, exiting')
  sys.exit(0)

#
# main processing loop:
#
cmd = prompt()

while cmd != 0:
  #
  if cmd == 1:
    stats(bucketname, bucket, endpoint, dbConn)

  elif cmd == 2:
    users(dbConn)

  elif cmd == 3:
    assets(dbConn)

  elif cmd == 4:
    download(dbConn, bucket)

  elif cmd == 5:
    download(dbConn, bucket, display=True)

  elif cmd == 6:
    upload(dbConn, bucket)

  elif cmd == 7:
    add_user(dbConn, bucket)
    
  else:
    print("** Unknown command, try again...")
  #
  cmd = prompt()

#
# done
#
print()
print('** done **')
