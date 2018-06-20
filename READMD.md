### springframework 定时任务和异步任务 executor推断

#### 定时任务
##### org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor

容器启动完毕后触发onApplicationEvent方法


```java

@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if (event.getApplicationContext() == this.applicationContext) {
			// Running in an ApplicationContext -> register tasks this late...
			// giving other ContextRefreshedEvent listeners a chance to perform
			// their work at the same time (e.g. Spring Batch's job registration).
			finishRegistration();
		}
	}
	
```
调用finishRegistration方法

```java
private void finishRegistration() {
		if (this.scheduler != null) {
			this.registrar.setScheduler(this.scheduler);
		}

		if (this.beanFactory instanceof ListableBeanFactory) {
			Map<String, SchedulingConfigurer> beans =
					((ListableBeanFactory) this.beanFactory).getBeansOfType(SchedulingConfigurer.class);
			List<SchedulingConfigurer> configurers = new ArrayList<>(beans.values());
			AnnotationAwareOrderComparator.sort(configurers);
			for (SchedulingConfigurer configurer : configurers) {
				configurer.configureTasks(this.registrar);
			}
		}

		if (this.registrar.hasTasks() && this.registrar.getScheduler() == null) {
			Assert.state(this.beanFactory != null, "BeanFactory must be set to find scheduler by type");
			try {
				// Search for TaskScheduler bean...
				this.registrar.setTaskScheduler(resolveSchedulerBean(beanFactory, TaskScheduler.class, false));
			}
			catch (NoUniqueBeanDefinitionException ex) {
				logger.debug("Could not find unique TaskScheduler bean", ex);
				try {
					this.registrar.setTaskScheduler(resolveSchedulerBean(beanFactory, TaskScheduler.class, true));
				}
				catch (NoSuchBeanDefinitionException ex2) {
					if (logger.isInfoEnabled()) {
						logger.info("More than one TaskScheduler bean exists within the context, and " +
								"none is named 'taskScheduler'. Mark one of them as primary or name it 'taskScheduler' " +
								"(possibly as an alias); or implement the SchedulingConfigurer interface and call " +
								"ScheduledTaskRegistrar#setScheduler explicitly within the configureTasks() callback: " +
								ex.getBeanNamesFound());
					}
				}
			}
			catch (NoSuchBeanDefinitionException ex) {
				logger.debug("Could not find default TaskScheduler bean", ex);
				// Search for ScheduledExecutorService bean next...
				try {
					this.registrar.setScheduler(resolveSchedulerBean(beanFactory, ScheduledExecutorService.class, false));
				}
				catch (NoUniqueBeanDefinitionException ex2) {
					logger.debug("Could not find unique ScheduledExecutorService bean", ex2);
					try {
						this.registrar.setScheduler(resolveSchedulerBean(beanFactory, ScheduledExecutorService.class, true));
					}
					catch (NoSuchBeanDefinitionException ex3) {
						if (logger.isInfoEnabled()) {
							logger.info("More than one ScheduledExecutorService bean exists within the context, and " +
									"none is named 'taskScheduler'. Mark one of them as primary or name it 'taskScheduler' " +
									"(possibly as an alias); or implement the SchedulingConfigurer interface and call " +
									"ScheduledTaskRegistrar#setScheduler explicitly within the configureTasks() callback: " +
									ex2.getBeanNamesFound());
						}
					}
				}
				catch (NoSuchBeanDefinitionException ex2) {
					logger.debug("Could not find default ScheduledExecutorService bean", ex2);
					// Giving up -> falling back to default scheduler within the registrar...
					logger.info("No TaskScheduler/ScheduledExecutorService bean found for scheduled processing");
				}
			}
		}
        //启动所有的定时任务
		this.registrar.afterPropertiesSet();
	}
```

启动所有定时任务时若没有在容器中发现指定的executor则默认创建一个单线程executor

```java
protected void scheduleTasks() {
        //若没有指定,默认创建一个单线程executor
		if (this.taskScheduler == null) {
			this.localExecutor = Executors.newSingleThreadScheduledExecutor();
			this.taskScheduler = new ConcurrentTaskScheduler(this.localExecutor);
		}
		if (this.triggerTasks != null) {
			for (TriggerTask task : this.triggerTasks) {
				addScheduledTask(scheduleTriggerTask(task));
			}
		}
		if (this.cronTasks != null) {
			for (CronTask task : this.cronTasks) {
				addScheduledTask(scheduleCronTask(task));
			}
		}
		if (this.fixedRateTasks != null) {
			for (IntervalTask task : this.fixedRateTasks) {
				addScheduledTask(scheduleFixedRateTask(task));
			}
		}
		if (this.fixedDelayTasks != null) {
			for (IntervalTask task : this.fixedDelayTasks) {
				addScheduledTask(scheduleFixedDelayTask(task));
			}
		}
	}
```

#### 异步任务
##### org.springframework.scheduling.annotation.AsyncAnnotationAdvisor(此类为异步任务核心advisor)
##### org.springframework.scheduling.annotation.AnnotationAsyncExecutionInterceptor(此类为异步任务MethodInterceptor)


```java
public Object invoke(final MethodInvocation invocation) throws Throwable {
		Class<?> targetClass = (invocation.getThis() != null ? AopUtils.getTargetClass(invocation.getThis()) : null);
		Method specificMethod = ClassUtils.getMostSpecificMethod(invocation.getMethod(), targetClass);
		final Method userDeclaredMethod = BridgeMethodResolver.findBridgedMethod(specificMethod);
        //推断异步方法执行需要的executor
		AsyncTaskExecutor executor = determineAsyncExecutor(userDeclaredMethod);
		if (executor == null) {
			throw new IllegalStateException(
					"No executor specified and no default executor set on AsyncExecutionInterceptor either");
		}

		Callable<Object> task = () -> {
			try {
				Object result = invocation.proceed();
				if (result instanceof Future) {
					return ((Future<?>) result).get();
				}
			}
			catch (ExecutionException ex) {
				handleError(ex.getCause(), userDeclaredMethod, invocation.getArguments());
			}
			catch (Throwable ex) {
				handleError(ex, userDeclaredMethod, invocation.getArguments());
			}
			return null;
		};

		return doSubmit(task, executor, invocation.getMethod().getReturnType());
	}
```

```java
protected AsyncTaskExecutor determineAsyncExecutor(Method method) {
        //首先从缓存中获取
		AsyncTaskExecutor executor = this.executors.get(method);
		if (executor == null) {
			Executor targetExecutor;
			String qualifier = getExecutorQualifier(method);
			//如果在@Async注解中指定executor名称,则从容器中直接获取
			if (StringUtils.hasLength(qualifier)) {
				targetExecutor = findQualifiedExecutor(this.beanFactory, qualifier);
			}
			else {
			    //指定为默认的executor
				targetExecutor = this.defaultExecutor;
				if (targetExecutor == null) {
					synchronized (this.executors) {
						if (this.defaultExecutor == null) {
						    //若没有指定默认的executor,从容器中按照指定优先级获取,若果容器中无法获取，则默认采用单线程executor
							this.defaultExecutor = getDefaultExecutor(this.beanFactory);
						}
						targetExecutor = this.defaultExecutor;
					}
				}
			}
			if (targetExecutor == null) {
				return null;
			}
			executor = (targetExecutor instanceof AsyncListenableTaskExecutor ?
					(AsyncListenableTaskExecutor) targetExecutor : new TaskExecutorAdapter(targetExecutor));
			this.executors.put(method, executor);
		}
		return executor;
	}
	
```

```java
/**
* 首先从容器中获取，如果无法获取则默认采用单线程
*/
protected Executor executor(@Nullable BeanFactory beanFactory) {
		Executor defaultExecutor = super.getDefaultExecutor(beanFactory);
		return (defaultExecutor != null ? defaultExecutor : new SimpleAsyncTaskExecutor());
}
```