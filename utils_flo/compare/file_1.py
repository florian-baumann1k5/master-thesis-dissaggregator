temp_cal = temp_calender_df.copy()
temp_cal = temp_cal[['Date', 'Tagestyp', str(lk)]].set_index("Date")
last_hour = temp_cal.copy()[-1:]
last_hour.index = last_hour.index + timedelta(1)
temp_cal = pd.concat([temp_cal, last_hour]).resample('h').ffill()[:-1]
temp_cal['Stunde'] = pd.DatetimeIndex(temp_cal.index).time
temp_cal = temp_cal.set_index(["Tagestyp", str(lk), 'Stunde'])
f = ('Lastprofil_{}.xls'.format(slp))
slp_profil = pd.read_excel(data_in('temporal', 'Gas Load Profiles', f))
slp_profil = pd.DataFrame(slp_profil.set_index(['Tagestyp', 'Temperatur\nin °C\nkleiner']))
slp_profil.columns = pd.to_datetime(slp_profil.columns, format='%H:%M:%S')
slp_profil.columns = pd.DatetimeIndex(slp_profil.columns).time
slp_profil = slp_profil.stack()
temp_cal['Prozent'] = [slp_profil[x] for x in temp_cal.index]
gas_total[int(lk)] = (ts_total[lk].values * temp_cal['Prozent'].values/100)