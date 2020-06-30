include("/home/francio-pc/tesi/JuliaScript/modFitsIO.jl")
using DataFrames, .fitsIO, DataFramesMeta

# Legge dal dump tutti le informazioni extra
f = open("/home/francio-pc/path_update.txt", "w")
write(f, "id|comp|path|RAs|DECs|z_spec|qflag|otype")
write(f, "\n")
close(f)

println("Hai scaricato la versione aggiornata di observations.txt?")
run(pipeline(`/home/francio-pc/tesi/clean_observations.sh`,
    stdin = "/home/francio-pc/Observations.txt",
    stdout = "/home/francio-pc/path_update.txt", append = true))

df_dump_txt = create_dataframe("/home/francio-pc/path_update.txt", 't')
df_dump_fits = create_dataframe("/home/francio-pc/observations_to_observed.fits", 'f')
println("DataFrame creato, yay!")

# Seleziono solo le colonne myid e path
select!(df_dump_txt, [:myid, :comp, :path])

# Seleziono solo le righe che hanno comp = " "
df_dump_fits = @where(df_dump_fits, :comp .== " ")
df_dump_txt = @where(df_dump_txt, :comp .== ".")

select!(df_dump_txt, Not(:comp))
select!(df_dump_fits, Not(:comp))

df = join(df_dump_fits, df_dump_txt, on = :myid)

write(df, "/home/francio-pc/Observed_$(get_current_date()).fits")
run(`rm -f /home/francio-pc/observations_to_observed.fits`)
run(`rm -f /home/francio-pc/path_update.txt`)
