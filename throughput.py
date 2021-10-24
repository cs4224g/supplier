import numpy as np
import pandas as pd

if __name__ == '__main__':
    stats = pd.read_csv('client.csv',dtype=str, header=None, usecols=[3])
    throughput = stats.astype(float)
    output = pd.DataFrame([[np.amax(throughput).to_string(index=False).strip(), np.amin(throughput).to_string(index=False).strip(), np.average(throughput)]])
    output.to_csv('throughput.csv', index=False, header=False)