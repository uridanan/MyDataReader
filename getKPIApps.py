import mydatareader

def getBIData():
    return mydatareader.getMapFromDB('redshift','bidbconfig.json','avgdailyimpressions.sql',1,2)


def getAppsDBData():
    return mydatareader.getMapFromDB('redshift', 'appsdbconfig.json', 'kpiAppsFromAppsDB.sql', 1, 2)


def main():
    print("Start")

    # Title
    # BundleId
    # Store
    # Account
    # Platform
    # Graphical Engine
    # Studio
    # Orientation
    # SDK
    # PSDK
    # Update
    # InitialRelease
    appsDB = getAppsDBData()

    # title
    # bundleid
    # store
    # banners_avg_daily_imp
    # inter_avg_daily_imp
    # rv_avg_daily_imp
    bi = getBIData()

    merge = mydatareader.join(bi,appsDB)
    r2c = mydatareader.transpose(merge)
    #export2csv('out.csv', r2c)
    mydatareader.exportcolumns2csv('out.csv',r2c,['title', 'bundleid', 'store', 'account', 'platform',
                                     'graphical engine', 'studio', 'orientation',
                                     'sdk', 'psdk', 'update', 'initialrelease',
                                     'banners_avg_daily_imp', 'inter_avg_daily_imp', 'rv_avg_daily_imp'])


main()