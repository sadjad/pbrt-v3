/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_ENGINE_H
#define PBRT_EXECUTION_ENGINE_H

#include <string>
#include <stdexcept>
#include <functional>

#include "loop.h"
#include "response.h"

class ExecutionEngine
{
public:
  /* success_callback( source_hash, target_hash, estimated_cost ) */
  using SuccessCallbackFunc =  std::function<void( const std::string &,
                                                   std::vector<gg::ThunkOutput> &&,
                                                   const float )>;

  /* failure_callback( source_hash, failure_reason ) */
  using FailureCallbackFunc =  std::function<void( const std::string &,
                                                   const JobStatus )>;

protected:
  SuccessCallbackFunc success_callback_ {};
  FailureCallbackFunc failure_callback_ {};

  size_t max_jobs_ { 0 };

public:
  ExecutionEngine( const size_t max_jobs = 1 )
    : max_jobs_( max_jobs )
  {
    if ( max_jobs == 0 ) {
      throw std::runtime_error( "max jobs cannot be zero" );
    }
  }

  void set_success_callback( SuccessCallbackFunc func ) { success_callback_ = func; }
  void set_failure_callback( FailureCallbackFunc func ) { failure_callback_ = func; }

  virtual void init( ExecutionLoop & ) {}
  virtual bool is_remote() const = 0;
  virtual size_t job_count() const = 0;
  size_t max_jobs() const { return max_jobs_; }
  virtual std::string label() const = 0;

  virtual ~ExecutionEngine() {}
};

#endif /* PBRT_EXECUTION_ENGINE_H */
