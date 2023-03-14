import cf
import numpy as np
import cartopy.crs as ccrs
import matplotlib.pyplot as plt

f1 = cf.read('../../DATOS/GEFSv12/apcp_sfc/2000/2000010500/d1-10/apcp_sfc_2000010500_c00.nc')
f2 = cf.read('../../DATOS/GEFSv12/apcp_sfc/2000/2000010500/d10-35/apcp_sfc_2000010500_c00.nc')
#f3 = cf.read('a025.nc')
#f4 = cf.read('a05.nc')

#pptest0 = f3[0]
#pptest1 = f4[0]

pp025 = f1[0]
pp05 = f2[0]

pp05_reg = pp05.regrids(pp025, 'conservative')
#print(pp05.dump())
#print(pp025.dump())
#print(pptest.dump())
print(pp05_reg.array.shape)
lat0 = pp05_reg.coordinate('latitude').array
lon0 = pp05_reg.coordinate('longitude').array

lat1 = pp05.coordinate('latitude').array
lon1 = pp05.coordinate('longitude').array

for i in np.arange(0,100):
    
    vreg = pp05_reg.array[i,:,:]
    vori = pp05.array[i,:,:]
    #print(np.sum(vreg*7.29e8))
    print(np.round(np.sum(vreg*7.29e8)/np.sum(vori*2.916e9), 4))
    
    fig, ax = plt.subplots(nrows=1, ncols=2, subplot_kw={'projection': ccrs.PlateCarree()})

    ax[0].pcolormesh(lon0, lat0, vreg, transform=ccrs.PlateCarree(), cmap='gist_rainbow',vmin=0, vmax=50)
    ax[0].coastlines()

    im = ax[1].pcolormesh(lon1, lat1, vori, transform=ccrs.PlateCarree(), cmap='gist_rainbow',vmin=0, vmax=50)
    ax[1].coastlines()

    fig.subplots_adjust(right=0.8)
    cbar_ax = fig.add_axes([0.85, 0.15, 0.05, 0.7])
    fig.colorbar(im, cax=cbar_ax)
    plt.savefig('./figuras/' + str(i) + '.jpg', dpi=150)
    plt.close(fig)




f1.close()
f2.close()
#f3.close()
#f4.close()
