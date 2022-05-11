import h2o
from h2o.grid.grid_search import H2OGridSearch
from h2o.estimators import H2OXGBoostEstimator

h2o.init(max_mem_size_GB=10)


def get_target_and_predictors(dataframe, target="label", exclude:list=[], verbose=False):
    predictors = dataframe.columns
    predictors = list(filter(lambda x: x not in exclude + [target], predictors))
    if verbose:
        print(f'target: {target}')
        print(f'response: {predictors}')
    return target, predictors


def grid_search_xgb(x, y, train, valid, grid_params):
    # Train and validate a cartesian grid of GBMs
    xgb_grid = H2OGridSearch(model=H2OXGBoostEstimator,
                             grid_id='xgb_grid',
                             hyper_params=grid_params)
    xgb_grid.train(
        x=x, y=y,
        training_frame=train,
        validation_frame=valid,
        seed=35
    )

    # Get the grid results, sorted by validation RMSE
    xgb_grid_perf = xgb_grid.get_grid(sort_by='RMSE', decreasing=False)
    return xgb_grid, xgb_grid_perf


def write_prediction_to_file(predictions_hf, output_path):
    p_list = h2o.as_list(predictions_hf)['predict'].tolist()

    with open(output_path, 'w') as f:
        for item in p_list:
            f.write("%s\n" % item)

    print(f' T/T+F Ratio: {p_list.count(True) / (p_list.count(False) + p_list.count(True))}')
