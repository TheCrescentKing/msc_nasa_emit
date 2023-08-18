import numpy as np
import xarray as xr
import pandas as pd

def preProcessData(reflectanceFilePath,
                   mineralFilePath,
                   groundTruthGroup = 1,
                   removeSingleInstanceValues = True,
                   applyDataBalancing = True,
                   trimDataPoints = True,
                   scaleData = True,
                   SCALE = 1):
	refl = xr.open_dataset(reflectanceFilePath)
	SCALE = 1
	cropDowntrack = int(refl.dims['downtrack']*SCALE)
	cropCrosstrack = int(refl.dims['crosstrack']*SCALE)
	refl = refl.sel(downtrack=slice(0, cropDowntrack), crosstrack=slice(0, cropCrosstrack))
	hsiData = refl['reflectance'].values
	del refl
	minerals = xr.open_dataset(mineralFilePath)
	minerals = minerals.sel(downtrack=slice(0, cropDowntrack), crosstrack=slice(0, cropCrosstrack))
	groundTruth = minerals['group_' + groundTruthGroup + '_mineral_id'].values
	groundTruth.shape
	del minerals
	X = hsiData.reshape(-1, hsiData.shape[2])
	y = groundTruth.ravel()
	X = pd.DataFrame(X)
	X['gt'] = y
	X = X.loc[:, ~(X == -0.01).any()]
	X = X.query('gt != 0')
	if(removeSingleInstanceValues):
		X = X[X['gt'].map(X['gt'].value_counts()) > 2]
	y = X.pop('gt').values
	if(applyDataBalancing):
		from imblearn.over_sampling import SMOTE
		oversample = SMOTE(k_neighbors=2)
		X, y = oversample.fit_resample(X, y)
	if(trimDataPoints):
		from sklearn.model_selection import train_test_split
		_, X, _, y = train_test_split(X, y, test_size=0.05, random_state=42, stratify=y)
	if(scaleData):
		from sklearn.preprocessing import StandardScaler
		scaler = StandardScaler()
		X = scaler.fit_transform(X)
	return X, y
	