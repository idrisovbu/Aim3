library(Rcpp)
cppFunction('NumericVector dcomp(NumericMatrix X, NumericMatrix Y){
                  int m = X.nrow();
                  int n = X.ncol();
                  int i;
                  int j;
                  int k;
                  IntegerVector q(n);
                  int kcopy;
                  int size_q;
                  NumericVector prod_x_in_y_out(m);
                  NumericVector prod_y_in_x_out(m);
                  IntegerVector denoms(n+1);
                  NumericMatrix result(m, n);
                  
                  denoms[0] = 0;
                  denoms[1] = n;
                  for(j=1; j<n; j++){
                    denoms[j+1] = denoms[j] * (n - j) / j;
                    for(i=0; i<m; i++){
                      result(i, j) = 0.0;
                    }
                  }
                  
                  for(k=1; k<pow(2, n); k++){
                    kcopy = k;
                    size_q = 0;
                    for(i=0; i<m; i++){
                      prod_x_in_y_out[i] = 1.0;
                      prod_y_in_x_out[i] = 1.0;
                    }
                    for(j=0; j<n; j++){
                      q[j] = kcopy % 2;
                      if(q[j] == 1){
                        size_q++;
                        kcopy--;
                        for(i=0; i<m; i++){
                          prod_x_in_y_out[i] = prod_x_in_y_out[i] * X(i, j);
                          prod_y_in_x_out[i] = prod_y_in_x_out[i] * Y(i, j);
                        }
                      } else{
                        for(i=0; i<m; i++){
                          prod_x_in_y_out[i] = prod_x_in_y_out[i] * Y(i, j);
                          prod_y_in_x_out[i] = prod_y_in_x_out[i] * X(i, j);
                        }
                      }
                      kcopy = kcopy/2;
                    }
                    for(j=0; j<n; j++){
                      if(q[j] == 1){
                        for(i=0; i<m; i++){
                          result(i, j) = result(i, j) + (prod_y_in_x_out[i] - prod_x_in_y_out[i])/denoms[size_q];
                        }
                      }
                    }
                  }
                  return result;
                }')

decompose <- function(dt, factor_names, start_year, end_year){
  dt[, paste0(factor_names, "_effect") := as.data.table(dcomp(as.matrix(.SD[, paste(factor_names, start_year, sep="_"), with=FALSE]),
                                                              as.matrix(.SD[, paste(factor_names, end_year, sep="_"), with=FALSE])))]
  dt[]
  return(dt)
}