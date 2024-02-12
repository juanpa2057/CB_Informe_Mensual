import numpy as np
import pandas as pd
import datetime as dt

import scipy.stats as stats


import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "notebook"
pio.templates.default = "plotly_white"


class Configuration:
    PRIMARY_PALETTE = ['#d5752d', '#59595b']
    SECONDARY_PALETTE = ['#13a2e1', '#00be91', '#fff65e', '#003fa2', '#ca0045']
    FULL_PALETTE = PRIMARY_PALETTE + SECONDARY_PALETTE

    PLOTLY_COLOR_SCALE_0_TO_1 = [
        (0, '#FFFFFF'),
        (1, FULL_PALETTE[0])
    ]
    PLOTLY_COLOR_SCALE_NEG1_TO_1 = [
        (0, FULL_PALETTE[1]),
        (0.5, '#FFFFFF'),
        (1, FULL_PALETTE[0])
    ]

    CELSIA_FONT = 'Arial'
    PLOTLY_TITLE_FONT_SIZE = 12
    PLOTLY_TITLE_X = 0.5

    GOLDEN_RATIO = (1+(5)**0.5)/2
    JBOOK_PLOTLY_WIDTH = 725
    JBOOK_PLOTLY_HEIGHT = JBOOK_PLOTLY_WIDTH / GOLDEN_RATIO

    dct_dow = {
        0: 'lunes',
        1: 'martes',
        2: 'miércoles',
        3: 'jueves',
        4: 'viernes',
        5: 'sábado',
        6: 'domingo',
    }

class Cleaning:
    # abbreviated as cln

    def repair_energy_series(energy, tolerance=None, trust_dips=False):
        # Most of the time energy consumption shouldn't be noisy. This
        # is because power peaks don't consume much energy as they are,
        # by definition, short in duration.
        # However, something about the way some meters operate gives rise 
        # to nasty contextual anomalies. They're contextual because it's 
        # not about the individual values, but a pattern within a subset 
        # of the time series. These anomalies seem to fall into categories 
        # of either finite or non-finite shifts. Finite shifts can occur 
        # in the energy and time axis, while non-finite shifts appear to 
        # only occur in the energy axis.

        # A finite shift in energy is characterised by having both a rising
        # and a falling edge (it is possible to find a start and an end to
        # the anomaly) and, because energy shouldn't decrease, any time it
        # does we know an anomaly has occured and we can search for a match.
        # If a match is found we can classify it as a finite shift, otherwise
        # it's non-finite. If the shift is non-finite with a falling edge
        # it's a meter swap, otherwise it's a "boost".

        # A meter change will manifest as a negative non-finite shift in 
        # cummulative energy; meaning all energy thereafter is shifted down 
        # by the same amount. It is characterised by not presenting with a 
        # positive shift of similar magnitude neither before nor after.

        # If a finite shift in energy is short in duration, finding a match 
        # is as simple as looking for flipped edges that are close in 
        # magnitude and picking the nearest one (in the time axis). If no 
        # match can be found ahead of the dip, then we look behind it.
        # It gets tricky when the local shift is long because the energies 
        # of the rising and falling edges can fall outside of our tolerance.
        # Simply relaxing the tolerance can lead to bad matches when the 
        # anomalies are short, so it is necessary to compensate for the
        # generation during the anomalous period.

        # Time shifts are hard to pick out algorithmically. Many methods
        # can be proposed, but whether they are efficient or not is a
        # different matter entirely. Inefficient methods aren't scalable.
        # If time shifts could be detected reliably then it would be a matter
        # of shifting the timestamps until the dE/dt correlated well with the
        # active power... or just removing them entirely and imputing the data.
        # If the meter reports both energy and power, and the sampling rate
        # isn't too low (this threshold is to be determined), then the 
        # element-wise relative-error between the time-difference of the 
        # energy against the power might reliably detect time shifts.
        # It might also detect dips and plateaus where the energy stays
        # constant.

        # Once global and local anomalies have been removed, imputing the
        # missing data can be done using a combination of integrating the
        # active power, and simple interpolation. Linear interpolation is
        # straightforward, and if the data is missing at random and the gaps
        # are small, then the underlying PDF is approximately preserved.
        # It might be better to interpolate using a ML model that accounts
        # for seasonal behaviors like hour of day and day of week.
        # Just taking the hourly mean is probably a significantly better
        # way of interpolating than linear.This can then be scaled to match
        # the generation during the gap.

        if (~energy.index.is_monotonic_increasing):
            energy = energy.sort_index()

        initial_energy = energy[0]
        delta_e = energy.diff()
        energy_dips = delta_e[delta_e < 0]

        for timestamp_0, dip in energy_dips.iteritems():
            is_subsequent = (delta_e.index > timestamp_0)
            is_previous = (delta_e.index < timestamp_0)
            is_aprox_opposite = (abs(delta_e + dip) < tolerance * -dip)

            subsequent_candidates = delta_e[(is_subsequent & is_aprox_opposite)]
            previous_candidates = delta_e[(is_previous & is_aprox_opposite)]

            timestamp_1 = np.nan
            if (len(subsequent_candidates) > 0):
                # Is left edge. Pick the closest candidate
                # to the right to define the interval
                timestamp_1 = subsequent_candidates.index[0]
            elif (len(previous_candidates) > 0):
                # Is right edge. Pick the closest candidate
                # to the left and swap endpoints
                timestamp_1 = timestamp_0
                timestamp_0 = previous_candidates.index[-1]

            # If timestamp_1 remains NaN then no candidates
            # were found, and it is either a swap or a wall.
            # But because we are only checking for decreases
            # in cummulative energy, walls won't even enter
            # this loop. So it must be a meter swap.

            is_meter_swap = (timestamp_1 is np.nan)

            if is_meter_swap:
                is_after_swap = (energy.index >= timestamp_0)
                # add back the energy accumulated by the previous meter
                energy.loc[is_after_swap] = energy.loc[is_after_swap] - dip
            else:
                if (trust_dips is False):
                    is_within_peak_or_valley = (
                        (energy.index >= timestamp_0)
                        & (energy.index < timestamp_1)
                    )
                    # remove inside peaks or valleys
                    energy = energy[~is_within_peak_or_valley]
                else:
                    # remove anomalous deltas then cumsum to recover the series
                    delta_e = delta_e.drop(index=[timestamp_0, timestamp_1])

        if (trust_dips is True):
            # add back initial energy to recover starting point
            delta_e[0] = initial_energy
            return delta_e.cumsum()
        else:
            return energy

    def repair_monotonic_increasing_variable(df, max_iter=None, tolerance=None, trust_gaps=False):
        lst_df = []
        for device in set(df['device']):
            df_device = df.query("device == @device")
            for variable in set(df_device['variable']):
                df_device_variable = df_device.query("variable == @variable")

                # store a temporary copy to write over
                repaired_variable = df_device_variable['value'].copy()

                i = 0
                while ((i < max_iter) & (repaired_variable.diff() < 0).any()):
                    i += 1
                    repaired_variable = Cleaning.repair_energy_series(
                        repaired_variable,
                        tolerance=tolerance,
                        trust_dips=trust_gaps
                    )

                df_device_variable_repaired = pd.merge(
                    repaired_variable.to_frame(),
                    df_device_variable.drop(columns='value'),
                    left_index=True,
                    right_index=True,
                    how='left'
                )

            lst_df.append(df_device_variable_repaired)

        return pd.concat(lst_df)


    def recover_energy_from_consumption(df, new_varname=None):
        lst_df = []
        for device in set(df['device']):
            df_device = df.query("device == @device")
            for variable in set(df_device['variable']):
                df_device_variable = df_device.query("variable == @variable")

                if (~df_device_variable.index.is_monotonic_increasing):
                    df_device_variable = df_device_variable.sort_index()

                df_device_variable['value'] = df_device_variable['value'].cumsum()
                df_device_variable['variable'] = new_varname


                lst_df.append(df_device_variable)

        return pd.concat(lst_df)


    def linearly_interpolate_series(series, data_rate_in_minutes=None):
        # If a data rate isn't provided the method will infer it
        # but generally one doesn't interpolate until the data has
        # been cleaned, which implies the removal of data, so it
        # is more robust to compute the data rate before cleaning
        # (make sure to remove duplicates beforehand).
        if data_rate_in_minutes is None:
            data_rate_in_minutes = int(
                series
                .index
                .to_series()
                .diff()
                .mode()
                .astype('timedelta64[m]')
            )

        return series.resample(f"{data_rate_in_minutes}T").first().interpolate(method='slinear')


    def remove_outliers_by_zscore(df, zscore=None):
        lst_df = []
        for device in set(df['device']):
            df_subset = df.query("device == @device")
            for variable in set(df_subset['variable']):
                df_var = df_subset.query("variable == @variable").copy()

                z_scores = stats.zscore(df_var['value'], nan_policy='omit')
                lst_df.append(df_var[abs(z_scores) < zscore])

        return pd.concat(lst_df)


    def differentiate_single_variable(df, new_var_name, remove_gap_data=False):
        # When differentiating a time series with a fixed sampling rate
        # any gaps in data will distort the value of the first datum after
        # the gap. If this is energy, for instance, then the next mean power 
        # value will be for the entire gap and not the corresponding interval.
        # To fix this set remove_gap_data to True. This ensures that only
        # real interval powers are being returned. This is significant because
        # failing to do so will impact the distribution of the derivative, and
        # thus affect things like confidence intervals.

        lst_df = []
        for device in set(df['device']):
            df_subset = df.query("device == @device").copy()
            if (~df_subset.index.is_monotonic_increasing):
                df_subset = df_subset.sort_index()

            hour_deltas = df_subset.index.to_series().diff() / np.timedelta64(1, 'h')
            interval_mean_power = df_subset['value'].diff() / hour_deltas

            if ((remove_gap_data is True) and (len(hour_deltas.mode()) > 1)):
                data_rate = hour_deltas.mode()[0]
                # only return gapless results
                derivative = interval_mean_power[(hour_deltas <= data_rate)]
            else:
                derivative = interval_mean_power

            df_left = (
                derivative
                .to_frame()
                # .dropna(how='any')
                .rename(columns={0:'value'})
            )

            df_temp = pd.merge(
                df_left,
                df_subset.drop(columns='value'),
                left_index=True,
                right_index=True,
                how='left'
            )
            df_temp['variable'] = new_var_name

            lst_df.append(df_temp)

        return pd.concat(lst_df)


    def subset_discard_date_intervals(df, DCT_INTERVALS):
        # Pass a dictionary where the keys are the device labels
        # and the values are lists of intervals, where each
        # interval is itself a list:
        # example = {
        #   'device_label_01':[
        #       ['2022-01-01', 2022-01-15],
        #       ['2022-02-01', 2022-02-15],
        #   ],
        #   'device_label_02':[
        #       ['2022-06-15', 2022-01-30],
        #       ['2022-07-15', 2022-02-31],
        #   ]
        # }
        #
        # The above example defines the removal of all data from 
        # the first 15 days of January and February from 
        # device_label_01 and the last 15 days of June and July 
        # from device_label_02.

        set_devices_inner = (
            set(df.device)
            .intersection(set(DCT_INTERVALS.keys()))
        )

        for device in set_devices_inner:
            intervals_per_device = DCT_INTERVALS[device]
            if len(intervals_per_device) > 0:
                for interval in intervals_per_device:
                    is_bad_date = (
                        (df['device'] == device)
                        & (df.index >= interval[0])
                        & (df.index <= interval[1])
                    )
                    df = df[~is_bad_date]
            
        return df

class Processing:
    # abbreviated as pro

    # 2.5th Percentile
    def q_low(x):
        return x.quantile(0.025)


    # 97.5th Percentile
    def q_high(x):
        return x.quantile(0.975)


    def datetime_attributes(df):
        df['hour'] = df.index.hour
        df['day'] = df.index.day
        df['dow'] = df.index.dayofweek.map(Configuration.dct_dow)
        df['cont_dow'] = (24 * df.index.dayofweek + df.index.hour) / 24
        df['week'] = df.index.isocalendar().week
        df['month'] = df.index.month
        df['year'] = df.index.year
        return df

    def split_into_baseline_and_study(df, baseline=None, study=None, inclusive='both'):
        # slicing the wrong way can produce warnings or simply fail. Use this:
        is_baseline_range = df.index.to_series().between(baseline[0], baseline[1], inclusive=inclusive)
        is_study_range = df.index.to_series().between(study[0], study[1], inclusive=inclusive)
        return (df[is_baseline_range], df[is_study_range])
    
    def split_total(df, baseline=None, study=None, past_month=None, inclusive='both'):
        # slicing the wrong way can produce warnings or simply fail. Use this:
        is_baseline_range = df.index.to_series().between(baseline[0], baseline[1], inclusive=inclusive)
        is_study_range = df.index.to_series().between(study[0], study[1], inclusive=inclusive)
        is_pastmonth_range = df.index.to_series().between(past_month[0], past_month[1], inclusive=inclusive)
        return (df[is_baseline_range], df[is_study_range], df[is_pastmonth_range])

class Graphing:
    # abbreviated as grp

    def hex_to_rgb(hex, alpha=None):
        if (hex[0] == '#'):
            hex = hex[1:]
        rgb = []
        for i in (0, 2, 4):
            decimal = int(hex[i:i+2], 16)
            rgb.append(decimal)

        if (alpha == None):
            return f"rgb{tuple(rgb)}"
        else:
            return f"rgba{tuple(rgb + [alpha])}"

    def plot_typical_day_by_hour(df_subset, subset=None, title=None, legend=False, include_ci=False, fill_ci=True):
        idx = 0
        fig = go.Figure()
        for subset_period in set(df_subset[subset]):
            df_plot = df_subset[df_subset[subset] == subset_period]
            hex_color = Configuration.FULL_PALETTE[idx % len(Configuration.FULL_PALETTE)]
            idx += 1

            if (include_ci is True):
                fillcolor = Graphing.hex_to_rgb(hex_color, 0.2)
                line_color = Graphing.hex_to_rgb(hex_color, 0.0)
                if (fill_ci is False):
                    fillcolor = Graphing.hex_to_rgb(hex_color, 0.0)
                    line_color = Graphing.hex_to_rgb(hex_color, 0.5)

                fig.add_trace(go.Scatter(
                    x=pd.concat([df_plot['hour'], df_plot['hour'][::-1]]),
                    y=pd.concat([df_plot['q_high'], df_plot['q_low'][::-1]]),
                    fill='toself',
                    fillcolor=fillcolor,
                    line_color=line_color,
                    line=dict(dash='dash'),
                    showlegend=legend,
                    name=f"Intervalo para el periodo {subset_period}"
                ))

            fig.add_trace(go.Scatter(
                x=df_plot['hour'],
                y=df_plot['mean'],
                line_color=Graphing.hex_to_rgb(hex_color, 0.75),
                name=f"Promedio para el periodo {subset_period}",
                showlegend=legend,
            ))

        fig.update_layout(
            title=title,
            font_family=Configuration.CELSIA_FONT,
            font_size=Configuration.PLOTLY_TITLE_FONT_SIZE,
            font_color=Configuration.FULL_PALETTE[1],
            title_x=Configuration.PLOTLY_TITLE_X,
            width=Configuration.JBOOK_PLOTLY_WIDTH,
            height=Configuration.JBOOK_PLOTLY_HEIGHT,
            yaxis=dict(title_text="Potencia Activa [kW]"),
            xaxis=dict(
                title_text="Hora del día",
                tickmode='array',
                tickvals=list(range(0, 24)),
                # ticktext = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
            )
        )

        fig.update_traces(mode='lines')
        fig.update_xaxes(rangemode="tozero")
        fig.update_yaxes(rangemode="tozero")
        fig.show()


    def plot_typical_week_by_day(df_subset, subset=None, title=None, legend=False, include_ci=False, fill_ci=True):
        idx = 0

        fig = go.Figure()
        for subset_period in set(df_subset[subset]):
            df_plot = df_subset[df_subset[subset] == subset_period]
            hex_color = Configuration.FULL_PALETTE[idx % len(Configuration.FULL_PALETTE)]
            idx += 1

            if (include_ci is True):
                fillcolor = Graphing.hex_to_rgb(hex_color, 0.2)
                line_color = Graphing.hex_to_rgb(hex_color, 0.0)
                if (fill_ci is False):
                    fillcolor = Graphing.hex_to_rgb(hex_color, 0.0)
                    line_color = Graphing.hex_to_rgb(hex_color, 0.5)

                fig.add_trace(go.Scatter(
                    x=pd.concat([df_plot['cont_dow'], df_plot['cont_dow'][::-1]]),
                    y=pd.concat([df_plot['q_high'], df_plot['q_low'][::-1]]),
                    fill='toself',
                    fillcolor=fillcolor,
                    line_color=line_color,
                    line=dict(dash='dash'),
                    showlegend=legend,
                    name=f"Intervalo para el periodo {subset_period}",
                ))

            fig.add_trace(go.Scatter(
                x=df_plot['cont_dow'],
                y=df_plot['mean'],
                line_color=Graphing.hex_to_rgb(hex_color, 0.75),
                name=f"Promedio para el periodo {subset_period}",
                showlegend=legend,
            ))

        fig.update_layout(
            title=title,
            font_family=Configuration.CELSIA_FONT,
            font_size=Configuration.PLOTLY_TITLE_FONT_SIZE,
            font_color=Configuration.FULL_PALETTE[1],
            title_x=Configuration.PLOTLY_TITLE_X,
            width=Configuration.JBOOK_PLOTLY_WIDTH,
            height=Configuration.JBOOK_PLOTLY_HEIGHT,
            yaxis=dict(title_text="Potencia Activa [kW]"),
            xaxis=dict(
                title_text="Día de la semana",
                tickmode='array',
                tickvals=[0, 1, 2, 3, 4, 5, 6, 7],
                ticktext=['L', 'M', 'W',
                        'J', 'V', 'S', 'D']
            )
        )

        fig.update_traces(mode='lines')
        fig.update_xaxes(rangemode="tozero")
        fig.update_yaxes(rangemode="tozero")
        fig.show()


    def plot_typical_year_by_week(df_subset, subset=None, title=None, include_ci=False, fill_ci=True):
        idx = 0

        fig = go.Figure()
        for subset_period in set(df_subset[subset]):
            df_plot = df_subset[df_subset[subset] == subset_period]
            hex_color = Configuration.FULL_PALETTE[idx % len(Configuration.FULL_PALETTE)]
            idx += 1

            if (include_ci is True):
                fillcolor = Graphing.hex_to_rgb(hex_color, 0.2)
                line_color = Graphing.hex_to_rgb(hex_color, 0.0)
                if (fill_ci is False):
                    fillcolor = Graphing.hex_to_rgb(hex_color, 0.0)
                    line_color = Graphing.hex_to_rgb(hex_color, 0.5)

                fig.add_trace(go.Scatter(
                    x=pd.concat([df_plot['week'], df_plot['week'][::-1]]),
                    y=pd.concat([df_plot['q_high'], df_plot['q_low'][::-1]]),
                    fill='toself',
                    fillcolor=fillcolor,
                    line_color=line_color,
                    line=dict(dash='dash'),
                    showlegend=True,
                    name=f"Intervalo para el periodo {subset_period}"
                ))

            fig.add_trace(go.Scatter(
                x=df_plot['week'],
                y=df_plot['mean'],
                line_color=Graphing.hex_to_rgb(hex_color, 0.75),
                name=f"Promedio para el periodo {subset_period}",
                showlegend=True,
            ))

        fig.update_layout(
            title=title,
            font_family=Configuration.CELSIA_FONT,
            font_size=Configuration.PLOTLY_TITLE_FONT_SIZE,
            font_color=Configuration.FULL_PALETTE[1],
            title_x=Configuration.PLOTLY_TITLE_X,
            width=Configuration.JBOOK_PLOTLY_WIDTH,
            height=Configuration.JBOOK_PLOTLY_HEIGHT,
            yaxis=dict(title_text="Potencia Activa [kW]"),
            xaxis=dict(
                title_text="Semana del año",
                # tickmode='array',
                # tickvals=list(range(0, 52)),
            )
        )

        fig.update_traces(mode='lines')
        fig.update_xaxes(rangemode="tozero")
        fig.update_yaxes(rangemode="tozero")
        fig.show()


    def compare_baseline_day_by_hour(df_bl, df_st, title=None, bl_label=None, st_label=None, bl_ci_label=None, include_ci=False, fill_ci=True):
        fig = go.Figure()
        hex_color_primary = Configuration.FULL_PALETTE[0]
        hex_color_secondary = Configuration.FULL_PALETTE[1]
        if (include_ci is True):
            fillcolor = Graphing.hex_to_rgb(hex_color_secondary, 0.2)
            line_color = Graphing.hex_to_rgb(hex_color_secondary, 0.0)
            line_style = None
            if (fill_ci is False):
                fillcolor = Graphing.hex_to_rgb(hex_color_secondary, 0.0)
                line_color = Graphing.hex_to_rgb(hex_color_secondary, 0.5)
                line_style = dict(dash='dash')

            # plot confidence interval
            fig.add_trace(go.Scatter(
                x=pd.concat([df_bl['hour'], df_bl['hour'][::-1]]),
                y=pd.concat([df_bl['q_high'], df_bl['q_low'][::-1]]),
                fill='toself',
                fillcolor=fillcolor,
                line_color=line_color,
                line=line_style,
                showlegend=True,
                name=bl_ci_label
            ))

        # plot mean curve
        fig.add_trace(go.Scatter(
            x=df_bl['hour'],
            y=df_bl['mean'],
            line_color=Graphing.hex_to_rgb(hex_color_secondary, 0.75),
            name=bl_label,
            showlegend=True,
        ))

        # plot mean curve
        fig.add_trace(go.Scatter(
            x=df_st['hour'],
            y=df_st['mean'],
            line_color=Graphing.hex_to_rgb(hex_color_primary, 0.75),
            name=st_label,
            showlegend=True,
        ))

        fig.update_layout(
            title=title,
            font_family=Configuration.CELSIA_FONT,
            font_size=Configuration.PLOTLY_TITLE_FONT_SIZE,
            font_color=Configuration.FULL_PALETTE[1],
            title_x=Configuration.PLOTLY_TITLE_X,
            width=Configuration.JBOOK_PLOTLY_WIDTH,
            height=Configuration.JBOOK_PLOTLY_HEIGHT,
            yaxis=dict(title_text="Potencia Activa [kW]"),
            xaxis=dict(
                title_text="Hora del día",
                tickmode='array',
                tickvals=list(range(0, 24)),
                # ticktext = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
            )
        )

        fig.update_traces(mode='lines')
        fig.update_xaxes(rangemode="tozero")
        fig.update_yaxes(rangemode="tozero")
        fig.show()


    def compare_baseline_week_by_day(df_bl, df_st, title=None, bl_label=None, st_label=None, bl_ci_label=None, include_ci=False, fill_ci=True):
        fig = go.Figure()
        hex_color_primary = Configuration.FULL_PALETTE[0]
        hex_color_secondary = Configuration.FULL_PALETTE[1]
        if (include_ci is True):
            fillcolor = Graphing.hex_to_rgb(hex_color_secondary, 0.2)
            line_color = Graphing.hex_to_rgb(hex_color_secondary, 0.0)
            line_style = None
            if (fill_ci is False):
                fillcolor = Graphing.hex_to_rgb(hex_color_secondary, 0.0)
                line_color = Graphing.hex_to_rgb(hex_color_secondary, 0.5)
                line_style = dict(dash='dash')


            fig.add_trace(go.Scatter(
                x=pd.concat([df_bl['cont_dow'], df_bl['cont_dow'][::-1]]),
                y=pd.concat([df_bl['q_high'], df_bl['q_low'][::-1]]),
                fill='toself',
                fillcolor=fillcolor,
                line_color=line_color,
                line=line_style,
                showlegend=True,
                name=bl_ci_label
            ))

        # plot mean curve
        fig.add_trace(go.Scatter(
            x=df_bl['cont_dow'],
            y=df_bl['mean'],
            line_color=Graphing.hex_to_rgb(hex_color_secondary, 0.75),
            name=bl_label,
            showlegend=True,
        ))

        # plot mean curve
        fig.add_trace(go.Scatter(
            x=df_st['cont_dow'],
            y=df_st['mean'],
            line_color=Graphing.hex_to_rgb(hex_color_primary, 0.75),
            name=st_label,
            showlegend=True,
        ))

        fig.update_layout(
            title=title,
            font_family=Configuration.CELSIA_FONT,
            font_size=Configuration.PLOTLY_TITLE_FONT_SIZE,
            font_color=Configuration.FULL_PALETTE[1],
            title_x=Configuration.PLOTLY_TITLE_X,
            width=Configuration.JBOOK_PLOTLY_WIDTH,
            height=Configuration.JBOOK_PLOTLY_HEIGHT,
            yaxis=dict(title_text="Potencia Activa [kW]"),
            xaxis=dict(
                title_text="Día de la semana",
                tickmode='array',
                tickvals=[0, 1, 2, 3, 4, 5, 6, 7],
                ticktext=['L', 'M', 'W',
                        'J', 'V', 'S', 'D']
            )
        )

        fig.update_traces(mode='lines')
        fig.update_xaxes(rangemode="tozero")
        fig.update_yaxes(rangemode="tozero")
        fig.show()


    def pivoted_dataframe_to_plotly_heatmap(df):
        return {'x': df.columns.tolist(),
                'y': df.index.tolist(),
                'z': df.values.tolist()}

    def hourly_heatmap(data, title=None):
        colorscale = Configuration.PLOTLY_COLOR_SCALE_0_TO_1
        minval = np.nanmin(data['z'])
        if (minval < 0):
            maxval = np.nanmax(data['z'])
            delta = maxval - minval
            zero = abs(minval) / abs(delta)
            colorscale = [
                (0, Configuration.FULL_PALETTE[1]),
                (zero, '#FFFFFF'),
                (1, Configuration.FULL_PALETTE[0])
            ]
            
        fig = go.Figure(
            data=go.Heatmap(
                data,
                colorscale=colorscale
            )
        )

        fig.update_yaxes(autorange='reversed') 
        fig.update_layout(
            title=title,
            font_family=Configuration.CELSIA_FONT,
            font_size=Configuration.PLOTLY_TITLE_FONT_SIZE,
            font_color=Configuration.FULL_PALETTE[1],
            title_x=Configuration.PLOTLY_TITLE_X,
            width=Configuration.JBOOK_PLOTLY_WIDTH,
            height=Configuration.JBOOK_PLOTLY_HEIGHT,
            yaxis=dict(title_text="Día del mes"),
            xaxis=dict(
                title_text="Hora del día",
                tickmode='array',
                tickvals=list(range(0,24))
            )
        )
        

        fig.show()