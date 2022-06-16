

#1 ESdown.py
	- Download and save to: f'Elasticsearch/ntia_{index_name}.csv' 
	NTIA_INIT_COLS = [
		'numISPfiber',
		'numISPother',
		'numISPwireless',
		'MaxConsumerDown98',
		'MaxConsumerUp98',
		] # LENGTH = 5 ; missing speedCatNtia, speedSourceNtia
	- Download and save to: f'Elasticsearch/mlab_prediction_{index_name}.csv' 
	MLAB_PREDICTION_COLS = [
    'CMC', 'Education', 'Health', ...]	 # LENGTH = 49

#2 ookla.ipynb | SEPARATELY: generated ookla tiles for all four 2021 QUARTERS in 1 go!
	- read ookla_fixed.zips, save to: ookla_state_tiles.to_file(f"{file_path}.geojson", driver="GeoJSON")
	- file ookla_gen_tiles.py:
		- read ookla_state_tiles
		- rare case: ookla: when devices > tests, set devices = tests
		- clean invalid latency
		- Quantile-based Flooring and Capping -- all raw Ookla values 'avg_d_kbps', 'avg_u_kbps', 'avg_lat_ms', 'devices', 'tests'
			- NOTE: NEXT TIME: do not Floor/Cap #devices and #tests
		- join to ookla tiles, at both CBG, and CB levels
		- speedSourceOokla @CBG = 'interpolatedFromTilesCBG' ; @CB  = 'joinedTilesAtCB'
        - save to ookla_state_tiles, both CBG and CB Ookla files:
        	- f"ookla_state_tiles/{QUARTER}_CBG_{sf}.csv"
        	- f"ookla_state_tiles/{QUARTER}_CB_{sf}.csv"

#3 ookla_ntia.py
	- Start from result of (#1) f'Elasticsearch/ntia_{index_name}.csv'
	- speedCatOokla, speedCatNtia
	* Now, CB_ookla_ntia = f"ookla_ntia/{QUARTER}_CB_{sf}.csv" is COMPLETE, contains GEOID + 20 cols:
		- OOKLA_END_COLS # LENGTH 13 = [8 x speeds, latency, device, test, cat, source] 
		- NTIA_END_COLS  # LENGTH 7 =  [3 x numISPs, maxUp, maxDown, cat, source]
	- Save 100%-filled CB_ookla_ntia to f"ookla_ntia/{QUARTER}_CB_{sf}.csv"


LAST PART: MLAB
# Start early: a separate and slow process | bigQuery Mlab-project download each quarter's speeds+devices dfs

# LARGE COMPONENT: mlab_rolling.ipynb
	- for sf in SF52:
		- CB LEVEL:
			- Start from result of (#3): 100%-filled CB_ookla_ntia = pd.read_csv(f"ookla_ntia/{QUARTER}_CB_{sf}.csv")
			- Read CB_mlab_pred_df = pd.read_csv(f'Elasticsearch/mlab_prediction_{index_name}.csv'); fill 100% CB_mlab_pred_df
			- CB_df (100% filled, numeric only) = CB_ookla_ntia+CB_mlab_pred_df  (on=GEOID) 
		- from CB to CBG:
			- NOTE: OOKLA_INIT_COLS + NTIA_INIT_COLS + MLAB_PREDICTION_COLS = 65 = CBG_df_medians+CBG_df_sums
			- CB_df.groupby(GEOID_cbg), split into CBG_df_medians+CBG_df_sums
			- CBG_df = CBG_df_medians+CBG_df_sums
			- result of (#2): ookla_cbg = pd.read_csv(f"ookla_state_tiles/{QUARTER}_CBG_{sf}.csv")
				- ookla_cbg dont have/need speedCat/speedSource Ookla/NTIA
			- CBG_df.update(ookla_cbg)
	- all_cbg = 52 x CBG_df 
	- add TIGER geom to all_cbg, fill missing lat/lon using medians

	""" FUNCTION DEF: merge_mlab_dfs(speed, device): i.e. Clean mlab training dataset
		- rename mlab cols, fill missing MLAB_MID_COLS values (mostly #device, #tests)
		- remove invalid latency, loss rate; Capping/Flooring all MLAB_MID_COLS values (higher threshold for #test, #device, #latency)
	""" 

	- COUNTY LEVEL: 
		- mlab_county = merge_mlab_dfs(county_speed, county_device, 'GEOID_c')
		- add geom to mlab_county
		- 100% fill mlab_county

	- all_cbg has OOKLA_INIT_COLS + NTIA_INIT_COLS + MLAB_PREDICTION_COLS = 65 COLS
	- mlab_county has MLAB_MID_COLS (i.e. no speedSourceMlab, no speedCatMlab)
	
	- area_interpolate(source_df = mlab_county, target_df = all_cbg, variables = #device, #test)
	- all_cbg_NEW = all_cbg + area_interpolate
	- NOW: all_cbg_NEW = all_cbg + new mlab values: Mlab(#device, #test)
	- all_cbg_NEW = all_cbg_NEW + mlab_county[minmax_cols] i.e. + 100% filled Mlab(min, max)


	- CBG LEVEL (training dataset): 
		- mlab_cbg_combined = merge_mlab_dfs(cbg_speed, cbg_device, 'GEOID_cbg')
		- mlab_free_df = all_cbg_NEW.drop(MLAB_END_COLS, axis=1, errors='ignore')
		- cbg_training_df = mlab_cbg_combined.merge(mlab_free_df, how='inner', on='GEOID_cbg') 
		- then, for future quarters: save cbg_training_df.to_csv(f'mlab_speeds/{QUARTER}-TRAINING-CBG.csv', index=False)
		- ROLLING/Previous QUARTERs: 
			- additional_cbg_training = [pd.read_csv(f'mlab_speeds/{PREV_QUARTER}-TRAINING-CBG.csv')]
			- cbg_training_df_combined = pd.concat([cbg_training_df, additional_cbg_training])
			- use cbg_training_df_combined in only 1 place: cur_model, cur_scaler, cur_predictor = build_rf_model(...)
			- maintain cbg_training_df in all other subsequent calls

	- Build RF PREDICTION MODELS:
		- predict latency, lossrate, meanSpeeds
		- Use predicted meanSpeeds to predict medSpeeds
		- build_rf_model(cbg_training_df,...)
	- Make predictions:
		- for cur_response in ['latencyMlab', 'lossrateMlab', 
	        'meanUploadMbpsMlab', 'meanDownloadMbpsMlab', 
	         'medDownloadMbpsMlab', 'medUploadMbpsMlab',]
			all_cbg_NEW[cur_response] = ...
		- CLEAN the predicted values i.e. all_cbg_NEW:
			- replace invalid latency/lossrate with np.nan
			- because predicted values have low skewness >> can skip Capping/Flooring
			- thus far, all_cbg_NEW should contain no nulls
		- add different speedSourceMlab to all_cbg_NEW & cbg_training_df
		- Important: UPDATE: all_cbg_NEW.update(cbg_training_df)
		- Save [complete] all_cbg_NEW to f'mlab_speeds/{QUARTER}-COMPLETE-CBG.csv' 
		- NOTE: all_cbg_NEW dont need/have speedCatMlab

	- LAST: from complete-CBG to complete-CB
		- mlab_cbg_complete = all_cbg_NEW[['GEOID_cbg', 'speedSourceMlab'] + MLAB_MID_COLS]
		- individual state_cb_upload = CB_ookla_ntia = pd.read_csv(f"ookla_ntia/{QUARTER}_CB_{sf}.csv")
		- Again, state_cb_upload contains OOKLA_END_COLS#13 + NTIA_END_COLS#7 + GEOID
		- state_cb_upload = state_cb_upload.merge(mlab_cbg_complete, how='left', on='GEOID_cbg')
			- Generate new 'speedCatMlab'; Refresh state_cb_upload['speedCatOokla']
			- Generate new speedRankReadyRaw
			- rare case: MLAB-CB: when devices > tests, set devices = tests
			- TODO: check/fill nulls with medians


# FINAL STEP: upload_speed_test.py: 3-4 hours to upload a QUARTER!
	- main_df = read_csv(f'speed_ready_upload/{QUARTER}_{sf}.csv')
	- prefix QUARTER to columns names
	- coerce cols to integer types
	- UPLOAD to speed_test 

# FINAL FINAL step: upload_bossdata for 2021Q4


********** DONE *************



TIP: start testing with small diverse states, such as: 02 (AK), 10 (DE)
TIP: always check type (int; zfill str type) of GEOID_cbg/GEOID;  
	# this_df.GEOID = this_df.GEOID.astype(str).str.zfill(CB_LENGTH)
	# this_df['GEOID_cbg'] = this_df.GEOID.str[:CBG_LENGTH]


======== APPENDIX # 1: ALL THE COLUMNS:

# 5 cols
NTIA_INIT_COLS = [
'numISPfiber', 'numISPother', 'numISPwireless',
'MaxConsumerDown98', 'MaxConsumerUp98',
]
NTIA_END_COLS = [
'numISPfiber', 'numISPother', 'numISPwireless',
'MaxConsumerDown98', 'MaxConsumerUp98',
'speedCatNtia', 'speedSourceNtia',
]
# 11 cols
OOKLA_INIT_COLS = [
'maxDownloadMbpsOokla', 'maxUploadMbpsOokla',
'meanDownloadMbpsOokla', 'meanUploadMbpsOokla',
'medDownloadMbpsOokla', 'medUploadMbpsOokla',
'minDownloadMbpsOokla', 'minUploadMbpsOokla',
'latencyOokla',
'numDeviceOokla', 'numTestOokla',
]
OOKLA_END_COLS = [
'maxDownloadMbpsOokla', 'maxUploadMbpsOokla',
'meanDownloadMbpsOokla', 'meanUploadMbpsOokla',
'medDownloadMbpsOokla', 'medUploadMbpsOokla',
'minDownloadMbpsOokla', 'minUploadMbpsOokla',
'latencyOokla',
'numDeviceOokla', 'numTestOokla',
'speedCatOokla', 'speedSourceOokla',
]
# 8 cols: max, min, device, test
MLAB_INIT_COLS = [
'maxDownloadMbpsMlab', 'maxUploadMbpsMlab',
'minDownloadMbpsMlab', 'minUploadMbpsMlab',
'numDeviceDownloadMlab', 'numDeviceUploadMlab',
'numTestDownloadMlab', 'numTestUploadMlab',
]
MLAB_MID_COLS = [
'maxDownloadMbpsMlab', 'maxUploadMbpsMlab',
'minDownloadMbpsMlab', 'minUploadMbpsMlab',
'numDeviceDownloadMlab', 'numDeviceUploadMlab',
'numTestDownloadMlab', 'numTestUploadMlab',
'meanDownloadMbpsMlab', 'meanUploadMbpsMlab',
'medDownloadMbpsMlab', 'medUploadMbpsMlab',
'latencyMlab', 'lossrateMlab',
] 
# 16 cols
MLAB_END_COLS = [
'maxDownloadMbpsMlab', 'maxUploadMbpsMlab',
'minDownloadMbpsMlab', 'minUploadMbpsMlab',
'numDeviceDownloadMlab', 'numDeviceUploadMlab',
'numTestDownloadMlab', 'numTestUploadMlab',
'meanDownloadMbpsMlab', 'meanUploadMbpsMlab',
'medDownloadMbpsMlab', 'medUploadMbpsMlab',
'latencyMlab', 'lossrateMlab',
'speedCatMlab','speedSourceMlab',
]

# 49 COLS
MLAB_PREDICTION_COLS = [
    'CMC', 'Education', 'Health', 
    'POP2019', 'Public Admin', 'age65overper', 'asianper', 'bachelorper', 
    'blackper', 'hh2020', 'hu2020', 'landareaSqmi', 'lengthMile', 
    'maxadownFiber', 'maxadownOther', 'maxadownWireless', 'maxadupFiber', 'maxadupOther', 'maxadupWireless',
    'mhincome', 'nativeper', 'nocomputerper_ct', 'nointernetper', 'nointernetper_ct', 
    'numISPcomm', 'numISPresi', 'num_household', 'num_household_ct', 'num_housingunit', 'otherraceper', 
    'parcelNumAgri', 'parcelNumCommer', 'parcelNumInfra', 'parcelNumResi', 
    'parcelNumRem', 'parcelNumValid', 'parcelNumTotal',
    'parcelBuildingCount', 'parcelBuildingFootprint',
    "cafiiLocation", 'pop2020', 'povertybelow15', 'povertybelow15_ct', 
    'povertybelow20_ct', 'povertyper', 'povertyper_ct', 
    'rdofLocation', 'rdofReserve', 'whiteper'] 

SPEED_TEST_INDEX_BASE_COLS = ['GEOID', 'statefips']

QUARTER_PREFIX_COLS = set(NTIA_END_COLS + OOKLA_END_COLS + MLAB_END_COLS + ['speedRankReadyRaw'])




