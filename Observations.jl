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

# Legge dal dump tutti le informazioni extra
df_dump = create_dataframe("/home/francio-pc/fits_sistemare/dump_observations.txt", 't')
df_coord = create_dataframe("/home/francio-pc/fits_sistemare/dump_observations.fits", 'f')
println("DataFrame creato, yay!")

# Elenco delle chiavi che mi servono per completare l'elenco
header_ref = ["TELESCOP", "INSTRUME"]
#header_ref = ["RA", "DEC", "DATE-OBS", "INSTRUME", "MJD-OBS"]

# Prendo tutti i path degli spettri da trasformare. Ricorda che sono scritti nella forma
#  folder/file.fits
# e manca quindi il percorso relativo.
path_list = df_dump[:, :path]
comp_list = df_dump[:, :comp] # Prendo la lista di comp
select!(df_dump, Not(:path))
select!(df_dump, Not(:comp))

# Unisco le coordinate così sono già pronte
select!(df_coord, [:myid, :RAd, :DECd])

# Controllo che non ci siano mismatch nel numero di file nel file di testo (dump) e l'elenco
# di path
if length(df_dump.:myid) != length(path_list)
    error("Il numero di oggetti non coincide tra gli input.")
else
    println("Tutto a posto!")
end

# Itero su tutti i file fits che devo includere nel catalogo.
# Nota che path_list deve essere adattata sul momento.
name_dict = read_dict_names()

for (i, path) in enumerate(path_list)
    folder, spec_name = rsplit(path, '/', limit = 2)
    path = "~/fits_sistemare/sistemare/fits_giorgio/"*path*".fits"
    header_vals = get_header_values(path, header_ref)
    new_id = create_new_id(df_coord[!, :RAd][i], df_coord[!, :DECd][i], name_dict,  comp_list[i])
    header_vals[1], header_vals[2] = df_coord[!, :RAd][i], df_coord[!, :DECd][i]
    new_spectra_df!(df_update, header_vals, new_id, i, comp_list[i])

    # Prepara i percorsi e copia i file
    ql_id = strip(string(df_dump[!, :id][i])*comp_list[i], '.')
    ql_path = "~/fits_sistemare/sistemare/ql_giorgio/"*folder*"/ql_$ql_id.fits"
    df_spec = create_dataframe(ql_path, 'f')
    df_spec_tot = complete_spec(df_spec, spec_name)
    write(df_spec_tot, "/home/francio-pc/fits_sistemare/sistemati/$new_id.fits")
end

update_dict(name_dict)
println("Dizionario aggiornato, scrittura fits")

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

write(complete_df, "/home/francio-pc/Observations_$(get_current_date()).fits")
