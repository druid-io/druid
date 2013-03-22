package com.metamx.druid.query.having;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.metamx.druid.input.Row;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * User: dyuan
 */
public class AndHavingSpec implements HavingSpec
{
  private List<HavingSpec> havingSpecs;

  @JsonCreator()
  public AndHavingSpec(@JsonProperty("havingSpecs") List<HavingSpec> havingSpecs) {
    Preconditions.checkArgument(havingSpecs != null && havingSpecs.size() >= 2, "There must be at least two operands for an 'and' operator");
    this.havingSpecs = ImmutableList.copyOf(havingSpecs);
  }

  @JsonProperty("havingSpecs")
  public List<HavingSpec> getHavingSpecs(){
    return havingSpecs;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AndHavingSpec that = (AndHavingSpec) o;

    if (havingSpecs != null ? !havingSpecs.equals(that.havingSpecs) : that.havingSpecs != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    return havingSpecs != null ? havingSpecs.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("AndHavingSpec");
    sb.append("{havingSpecs=").append(havingSpecs);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean eval(Row row)
  {
    for(HavingSpec havingSpec: havingSpecs) {
      if(!havingSpec.eval(row)){
        return false;
      }
    }

    return true;
  }
}
