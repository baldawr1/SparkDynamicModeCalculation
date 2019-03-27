# Spark Dynamic Mode Calculation - Calculate Mode across multiple columns(features) dynamically
Many times data scientist need to calculate mode (https://www.mathsisfun.com/definitions/mode.html) across various columns/features of a dataframe/dataset.  
The columns/features name change from one dataset to another.
The code takes in column names for which mode needs to be calculated and dynamically generate spark sql utilizing sparks catalyst optimizer for good performance.
