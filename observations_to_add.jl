include("/home/francio-pc/tesi/JuliaScript/modFitsIO.jl")
using .fitsIO, DataFrames

# Questo è il file che viene man mano aggiornato durante l'esecuzione.
df_update = DataFrame(myid = Int64[],
                      # id = Int64[],
                      jid = String[],
                      comp = String[],
                      RAd = Float64[],
                      DECd = Float64[],
                      RAs = String[],
                      DECs = String[],
                      # z_spec = Float64[],
                      # qflag = String[],
                      DateObs = String[],
                      DateObsMJD = Float64[],
                      # otype = String[],
                      Instrument = String[])

# prepara il file per la lettura
f = open("/home/francio-pc/fits_sistemare/sistemare/dump.txt", "w")
write(f, "id|otype|z_spec|qflag")
write(f, "\n")
close(f)
run(pipeline(`/home/francio-pc/tesi/clean_recap.sh`,
             stdin = "/home/francio-pc/fits_sistemare/sistemare/SUMMARY.dat",
             stdout = "/home/francio-pc/fits_sistemare/sistemare/dump.txt", append = true))

# Legge dal dump tutti le informazioni extra
# Il file di riferimento è il txt
df_dump = add_topcat_coord("/home/francio-pc/fits_sistemare/sistemare/dump.txt",
                           "/home/francio-pc/fits_sistemare/sistemare/dump.fits")

# Observations è l'ultima versione, quella a cui poi dovrai attaccare il file generato
# da questo script. Serve per controllare che non ci siano id duplicati che mandano in
# palla il sistema di nomi.
df_observations = create_dataframe("/home/francip-pc/Observations.fits", 'f')
id_observation = Set(df_observations[!, :id])

# df_dump = create_dataframe("/tmp/info.fits", 'f')

println("DataFrame creato, yay!")

# Elenco delle chiavi che mi servono per completare l'elenco
header_ref = ["TELESCOP", "INSTRUME"]
#header_ref = ["RA", "DEC", "DATE-OBS", "INSTRUME", "MJD-OBS"]

# Prendo tutti i path degli spettri da trasformare. Ricorda che sono scritti nella forma
#  folder/file.fits
# e manca quindi il percorso relativo.

ids = df_dump[!, :id]

# Unisco le coordinate così sono già pronte
df_coord = select(df_dump, [:id, :RAd, :DECd])

# Controllo che non ci siano mismatch nel numero di file nel file di testo (dump) e l'elenco
# di path
# if length(df_dump.:myid) != length(keys(path_dict))
#     error("Il numero di oggetti non coincide tra gli input.")
# else
#     println("Tutto a posto!")
# end

# Itero su tutti i file fits che devo includere nel catalogo.
# Nota che path_list deve essere adattata sul momento.
name_dict = read_dict_names()

# Nota che non hai un modo unico di determinare gli id, dipende dal nome in origine.
for (i, id) in enumerate(ids)
    path = "/home/francio-pc/fits_sistemare/sistemare/tmp/HBQS$(id)_sum1D.fits"
    if id in id_observation
        println(id) # Usalo per controllare i file
        comp = "u"
    else
        comp = "."
    end
    folder, spec_name = rsplit(path, '/', limit = 2)
    header_vals = get_header_values(path, header_ref)
    new_id = create_new_id(df_coord[!, :RAd][i], df_coord[!, :DECd][i], name_dict, comp)
    header_vals[1], header_vals[2] = df_coord[!, :RAd][i], df_coord[!, :DECd][i]
    new_spectra_df!(df_update, header_vals, new_id, i)

    # Prepara i percorsi e copia i file
    ndf = image_to_table(path)
    write(ndf, "/home/francio-pc/fits_sistemare/sistemati/$new_id.fits")
end

update_dict(name_dict)
println("Dizionario aggiornato, scrittura fits")

select!(df_dump, Not(:RAd))
select!(df_dump, Not(:DECd))

complete_df = join(df_update, df_dump, on = :myid)

select!(complete_df, :myid,
                     :id,
                     :jid,
                     :comp,
                     :RAd,
                     :DECd,
                     :RAs,
                     :DECs,
                     :z_spec,
                     :qflag,
                     :DateObs,
                     :DateObsMJD,
                     :otype,
                     :Instrument)

write(complete_df, "/home/francesco/to_add.fits")
