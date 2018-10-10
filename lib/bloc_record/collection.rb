module BlocRecord
  class Collection < Array
    def update_all(updates)
      ids = self.map(&:id)
      self.any? ? self.first.class.update(ids, updates) : false
    end

    def take(num=1)
      self.any? ? self.first.take(num) : false
    end

    def where(*args)
      self.any? ? self.first.where(args) : false
    end

    def where_not(*args)
      self.any? ? self.first.where_not(args) : false
    end
  end
end
