import numpy as np
import pandas as pd

if __name__ == '__main__':
    stats = pd.read_csv('client.csv',dtype=str)
    throughput = stats['measurement_c'].astype(float)
    output = pd.DataFrame([[np.amax(throughput), np.amin(throughput), np.average(throughput)]], columns=['min_throughput', 'max_throughput', 'avg_throughput'])
    output.to_csv('throughput.csv', index=False)