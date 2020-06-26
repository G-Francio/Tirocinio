include("/home/francio-pc/tesi/JuliaScript/modFitsIO.jl")
using Statistics, DataFrames, Gnuplot

function open_(str)
    df = create_dataframe(str, 'f')
    select!(df, [:jid, :z_spec])
end

tit = "Stacking";
df = open_("/home/francio-pc/spec_.fits");

grid_low = 900.;
grid_high = 2200.;
grid_step = 2;
wave_range = grid_low:grid_step:grid_high;

stack_list = Array{DataFrame, 1}();

for row in eachrow(df)
    df_ = create_dataframe("/home/francio-pc/spettri/$(row[:jid]).fits", 'f');
    if maximum(df_[!, :flux]) > 20*median(df_[!, :flux]) || minimum(df_[!, :flux]) < -20*median(df_[!, :flux])
        continue
    end
    push!(stack_list, prepare_stack(df_, row[:z_spec];
            grid_low_ = grid_low, grid_high_ = grid_high, grid_step_ = grid_step));
end

tot_stk = length(stack_list);
stack = DataFrame(wave = collect(wave_range), flux = zeros(Float32, length(wave_range)));
count = DataFrame(wave = collect(wave_range), count = zeros(Int64, length(wave_range)));

stack_spectra!(stack, count, stack_list);

@gp(title = "Stack ($tot_stk spettri), "*tit,  xlab = "Angstrom", ylabel = "FluxUnits",
    "set y2tics nomirror textcolor 'black'", "set ytics nomirror",
    xrange = [grid_low - 50, grid_high + 50],
    "set y2range [$(minimum(filter(!isnan, count[!, :count])) - 1):$(maximum(filter(!isnan, count[!, :count])) + 1)]",
    count[!, :wave], count[!, :count], "pt 7 ps 0.4 lc 'grey80' t 'Conteggi' axes x1y2",
    stack[!, :wave], stack[!, :flux], "w l lc 'red'  notitle",
    stack[!, :wave], stack[!, :flux], "pt 7 ps 0.4 lc 'blue' t 'Spettro'")

save(term = "pngcairo size 1280, 720 fontscale 0.8", output = "$tit.png")
write(stack, tit*".fits")
write(count, "count"*tit*".fits")
