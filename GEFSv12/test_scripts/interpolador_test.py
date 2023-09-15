import numpy as np
from scipy.interpolate import RegularGridInterpolator

x, y = np.array([-2, 0, 4]), np.array([-2, 0, 2, 5])
print(x.shape)
print(y.shape)
def ff(x, y):
    return x**2 + y**2
xg, yg = np.meshgrid(x, y, indexing='ij')
data = ff(xg, yg)
print(data.shape)
interp = RegularGridInterpolator((x, y), data, bounds_error=False, fill_value=None)


