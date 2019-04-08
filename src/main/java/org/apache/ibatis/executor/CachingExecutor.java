/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cache.TransactionalCacheManager;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

import java.sql.SQLException;
import java.util.List;

/**
 * 实现 Executor 接口，支持二级缓存的 Executor 的实现类
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public class CachingExecutor implements Executor {

  /**
   * 被委托的 Executor 对象(SimpleExecutor、ReuseExecutor、BatchExecutor)
   */
  private final Executor delegate;
  /**
   * TransactionalCacheManager 对象
   *
   * 支持事务的缓存管理器。因为二级缓存是支持跨 Session 进行共享，
   * 此处需要考虑事务，那么，必然需要做到事务提交时，才将当前事务中查询时产生的缓存，
   * 同步到二级缓存中。这个功能，就通过 TransactionalCacheManager 来实现。
   */
  private final TransactionalCacheManager tcm = new TransactionalCacheManager();

  /**
   * 构造器
   * @param delegate
   */
  public CachingExecutor(Executor delegate) {
    // 设置 delegate 属性，为被委托的 Executor 对象
    this.delegate = delegate;
    // 设置 delegate 被当前 CachingExecutor 执行器所包装
    delegate.setExecutorWrapper(this);
  }

  /**
   * 具体的实现代码，是直接调用委托执行器 delegate 的对应的方法
   * @return
   */
  @Override
  public Transaction getTransaction() {
    return delegate.getTransaction();
  }

  @Override
  public void close(boolean forceRollback) {
    try {
      //issues #499, #524 and #573
      // 如果强制回滚，则回滚 TransactionalCacheManager
      if (forceRollback) {
        tcm.rollback();
      } else {
        // 如果强制提交，则提交 TransactionalCacheManager
        tcm.commit();
      }
    } finally {
      // 执行 delegate 对应的方法
      delegate.close(forceRollback);
    }
  }

  /**
   * 具体的实现代码，是直接调用委托执行器 delegate 的对应的方法
   * @return
   */
  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public int update(MappedStatement ms, Object parameterObject) throws SQLException {
    // 如果需要清空缓存，则进行清空
    flushCacheIfRequired(ms);
    // 执行 delegate 对应的方法
    return delegate.update(ms, parameterObject);
  }

  @Override
  public <E> List<E> query(MappedStatement ms, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler) throws SQLException {
    // 获得 BoundSql 对象
    BoundSql boundSql = ms.getBoundSql(parameterObject);
    // 创建 CacheKey 对象
    CacheKey key = createCacheKey(ms, parameterObject, rowBounds, boundSql);
    // 查询
    return query(ms, parameterObject, rowBounds, resultHandler, key, boundSql);
  }

  @Override
  public <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) throws SQLException {
    // 如果需要清空缓存，则进行清空
    flushCacheIfRequired(ms);
    // 执行 delegate 对应的方法，无法开启二级缓存，所以只好调用 delegate 对应的方法
    return delegate.queryCursor(ms, parameter, rowBounds);
  }

  @Override
  public <E> List<E> query(MappedStatement ms, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler, CacheKey key, BoundSql boundSql)
      throws SQLException {
    // 获得 Cache 对象，即当前 MappedStatement 对象的二级缓存。
    Cache cache = ms.getCache();
    if (cache != null) {
      // 如果有 Cache 对象，说明该 MappedStatement 对象，有设置二级缓存
      // 如果需要清空缓存，则进行清空
      flushCacheIfRequired(ms);
      // isUseCache() 方法，返回 true 时，才使用二级缓存
      if (ms.isUseCache() && resultHandler == null) {
        // 暂时忽略，存储过程相关
        ensureNoOutParams(ms, boundSql);
        @SuppressWarnings("unchecked")
        // 从二级缓存中，获取结果
        List<E> list = (List<E>) tcm.getObject(cache, key);
        if (list == null) {
          // 如果不存在，则从数据库中查询
          list = delegate.query(ms, parameterObject, rowBounds, resultHandler, key, boundSql);
          // 缓存结果到二级缓存中，其实实际上，此处结果还没添加到二级缓存中，要事务的提交时才对二级缓存做修改
          tcm.putObject(cache, key, list); // issue #578 and #116
        }
        // 如果二级缓存中存在，则直接返回结果
        return list;
      }
    }
    // 如果没有 Cache 对象，说明该 MappedStatement 对象，未设置二级缓存，则从数据库中查询
    return delegate.query(ms, parameterObject, rowBounds, resultHandler, key, boundSql);
  }

  /**
   * 具体的实现代码，是直接调用委托执行器 delegate 的对应的方法
   * @return
   * @throws SQLException
   */
  @Override
  public List<BatchResult> flushStatements() throws SQLException {
    return delegate.flushStatements();
  }

  @Override
  public void commit(boolean required) throws SQLException {
    // 执行 delegate 对应的方法
    delegate.commit(required);
    // 提交 TransactionalCacheManager
    tcm.commit();
  }

  @Override
  public void rollback(boolean required) throws SQLException {
    try {
      // 执行 delegate 对应的方法
      delegate.rollback(required);
    } finally {
      if (required) {
        // 回滚 TransactionalCacheManager
        tcm.rollback();
      }
    }
  }

  private void ensureNoOutParams(MappedStatement ms, BoundSql boundSql) {
    if (ms.getStatementType() == StatementType.CALLABLE) {
      for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
        if (parameterMapping.getMode() != ParameterMode.IN) {
          throw new ExecutorException("Caching stored procedures with OUT params is not supported.  Please configure useCache=false in " + ms.getId() + " statement.");
        }
      }
    }
  }

  /**
   * 具体的实现代码，是直接调用委托执行器 delegate 的对应的方法
   * @param ms MappedStatement 对象
   * @param parameterObject
   * @param rowBounds
   * @param boundSql
   * @return
   */
  @Override
  public CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql) {
    return delegate.createCacheKey(ms, parameterObject, rowBounds, boundSql);
  }

  /**
   * 具体的实现代码，是直接调用委托执行器 delegate 的对应的方法
   * @param ms
   * @param key
   * @return
   */
  @Override
  public boolean isCached(MappedStatement ms, CacheKey key) {
    return delegate.isCached(ms, key);
  }

  /**
   * 具体的实现代码，是直接调用委托执行器 delegate 的对应的方法
   * @param ms MappedStatement 对象
   * @param resultObject
   * @param property
   * @param key
   * @param targetType
   */
  @Override
  public void deferLoad(MappedStatement ms, MetaObject resultObject, String property, CacheKey key, Class<?> targetType) {
    delegate.deferLoad(ms, resultObject, property, key, targetType);
  }

  /**
   * 具体的实现代码，是直接调用委托执行器 delegate 的对应的方法
   */
  @Override
  public void clearLocalCache() {
    delegate.clearLocalCache();
  }

  /**
   * 清空缓存
   *
   * `@Options(flushCache = Options.FlushCachePolicy.TRUE)` 或 `<select flushCache="true">`方式，开启需要清空缓存
   *
   * 注意，此时清空的仅仅是当前事务中查询数据产生的缓存。而真正的清空，在事务的提交时。
   * 这是为什么呢？因为二级缓存是跨 Session 共享缓存，在事务尚未结束时，不能对二级缓存做任何修改。
   * @param ms
   */
  private void flushCacheIfRequired(MappedStatement ms) {
    Cache cache = ms.getCache();
    // 是否需要清空缓存
    if (cache != null && ms.isFlushCacheRequired()) {
      tcm.clear(cache);
    }
  }

  @Override
  public void setExecutorWrapper(Executor executor) {
    throw new UnsupportedOperationException("This method should not be called");
  }

}
