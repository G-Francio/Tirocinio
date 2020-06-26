include("/home/francio-pc/tesi/JuliaScript/modFitsIO.jl")
using .fitsIO, DataFrames

odf = create_dataframe("/home/francio-pc/Observations.fits", 'f')
ndf = create_dataframe("/home/francio-pc/to_add.fits", 'f')

ddf = update_observation(odf, ndf)
name = "/home/francio-pc/Observations_$(get_current_date()).fits"
write(ddf, name)
run(`cp $name /home/francio-pc/observations_to_observed.fits`)
