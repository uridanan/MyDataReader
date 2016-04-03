SELECT t.Title, a.BundleId,
if(store.StoreName="App Store","Apple",if(store.StoreName="Google Play","GooglePlay",if(store.StoreName="Amazon Appstore","Amazon","Other"))) as Store,
c.AccountName,
if(t.Platform=3,"Native",if(t.Platform=2,"Unity",if(t.Platform=1,"GSDK",if(t.Platform=0,"TT Platform","")))) as 'Platform',
if(t.GraphicalEngine=3,"Cocos2D",if(t.GraphicalEngine=2,"Unity",if(t.GraphicalEngine=1,"Cocos2DX",if(t.GraphicalEngine=0,"None","")))) as 'Graphical Engine',
s.DisplayName as 'Studio',
if(t.orientation=1,"Portrait","Landscape") as 'Orientation',
ri.version as 'SDK', ri.PSDKVersion as 'PSDK', rai.releasedate as 'Update', frai.releasedate as 'InitialRelease'
FROM TabTale_DB.Title_Info as t,
Studio_Info as s,
Application_Account_Info as a,
Account_Info as c,
Store_Info as store,
Application_Release_Info as ri,
Application_Release_Info as fri,
App_Release_Account_Info as rai,
App_Release_Account_Info as frai
where s.StudioID = t.StudioID
and a.TitleID = t.TitleID
and a.AccountID = c.AccountID
and c.StoreID = store.StoreID
and a.status = 0
and ri.BuildDate = '2014-08-27' and ri.applicationid=rai.AppReleaseID
and fri.BuildDate = '2014-07-27' and fri.applicationid=frai.AppReleaseID
and ri.applicationid = fri.applicationid
and ri.applicationid = a.applicationid
