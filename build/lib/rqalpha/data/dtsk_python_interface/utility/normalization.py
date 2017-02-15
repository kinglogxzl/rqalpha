import numpy as np
import copy
from copy import deepcopy

#normalize the list into range [-1,1]
def linear_normalization(data):
    normalization = deepcopy(data)
    for index, datai in enumerate(normalization):
        datai = float(datai)
        normalization[index] = datai
    max = np.max(normalization)
    min = np.min(normalization)
    for index, datai in enumerate(data):
        datai = 2*(datai-min)/(max-min)-1
        normalization[index] = datai
    return normalization

#normalize the list by substracting mean and divided by standard deviation (zscore)
def z_score(data):
    normalization = deepcopy(data)
    average = np.mean(normalization)
    std = np.std(normalization)
    for index, datai in enumerate(normalization):
        datai = (datai-average)/std
        normalization[index] = datai
    return normalization


samplelist = [1, 2, 3, 4, 5]
linearsample = linear_normalization(samplelist)
zscoresample = z_score(samplelist)
