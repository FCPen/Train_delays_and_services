import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def numeric_col_distributions(dataframe: pd.DataFrame, numeric_cols: list):
    """
    This function returns histograms and boxplots for each numeric column in the provided dataframe.

    Args:

    - dataframe (pd.DataFrame): The input dataframe containing the data.
    - numeric_cols (list): A list of column names in the dataframe that are numeric.
    """
    for col in numeric_cols:
        #print("Column name: ",col)
    
        #print('Skew :',round(dataframe[col].skew(), 2))
    
        plt.figure(figsize = (15, 4))
    
        plt.subplot(1, 2, 1)
    
        #dataframe[col].hist(bins = 10, grid = False)
        sns.histplot(data=dataframe, x=col)
        
        plt.title(col + ',' + ' ' + 'Skew: {}'.format(np.round(dataframe[col].skew(), 2)))
    
        plt.ylabel('count')
        
        plt.xlabel(col)
    
        plt.subplot(1, 2, 2)
    
        sns.boxplot(x = dataframe[col])